namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.Linq;

    public class MsSqlServerMergeSqlStatementCreator : IAdoNetWriteToTableSqlStatementCreator
    {
        public string TableName { get; set; }
        public List<(string DbColumn, string RowColumn)> ColumnMap { get; set; }
        public string[] KeyColumns { get; set; }
        public string[] ValueColumns { get; set; }
        public bool UpsertKeyColumns { get; set; } = true;

        public IEnumerable<string> AllColumns => KeyColumns.Select(x => x + " (key").Union(ValueColumns.Select(x => x + " (value)"));

        private string[] _allColumns;
        private string _allColumnsConvertedAndJoined;
        private string _allColumnsConvertedAndJoinedSource;
        private string _valueColumnsConvertedAndJoined;
        private string _valueColumnsConvertedAndJoinedInsert;
        private string _keyColumnsConvertedAndJoined;
        private string _valueColumnsNames;
        private Dictionary<string, string> _map;

        public void Prepare(AdoNetWriteToTableOperation operation, IProcess process)
        {
            if (string.IsNullOrEmpty(TableName))
                throw new OperationParameterNullException(operation, nameof(TableName));
            if (KeyColumns == null)
                throw new OperationParameterNullException(operation, nameof(KeyColumns));
            if (ValueColumns == null)
                throw new OperationParameterNullException(operation, nameof(ValueColumns));
            _allColumns = KeyColumns.Concat(ValueColumns).ToArray();
            _allColumnsConvertedAndJoined = string.Join(", ", _allColumns.Select(x => GetColumnName(x)));
            _valueColumnsConvertedAndJoinedInsert = string.Join(", ", ValueColumns.Select(x => "source." + GetColumnName(x)));
            _valueColumnsNames = string.Join(", ", ValueColumns.Select(x => GetColumnName(x)));
            _allColumnsConvertedAndJoinedSource = string.Join(", ", _allColumns.Select(x => "source." + GetColumnName(x)));
            _valueColumnsConvertedAndJoined = string.Join(", ", ValueColumns.Select(x => GetColumnName(x) + " = source." + GetColumnName(x)));
            _keyColumnsConvertedAndJoined = string.Join(" AND ", KeyColumns.Select(x => "target." + GetColumnName(x) + " = source." + GetColumnName(x)));

            if (ColumnMap != null)
            {
                _map = new Dictionary<string, string>();
                foreach (var (dbColumn, rowColumn) in ColumnMap)
                {
                    _map[rowColumn] = dbColumn;
                }
            }
        }

        public string CreateRowStatement(ConnectionStringSettings settings, IRow row, AdoNetWriteToTableOperation op)
        {
            var startIndex = op.ParameterCount;
            foreach (var column in _allColumns)
            {
                op.CreateParameter(column, row[column]);
            }

            var statement = "(" + string.Join(", ", _allColumns.Select(x => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";

            if (row.Flagged)
                op.Process.Context.LogRow(op.Process, row, "sql statement generated: {SqlStatement}", statement);

            return statement;
        }

        public string GetColumnName(string rowColumnName)
        {
            var name = (_map?.Count > 0 && _map.TryGetValue(rowColumnName, out var mappedColumnName)) ? mappedColumnName : rowColumnName;
            return !name.StartsWith("[") ? "[" + name + "]" : name;
        }

        public string CreateStatement(ConnectionStringSettings settings, List<string> rowStatements)
        {
            return "MERGE INTO " + TableName + " target USING (VALUES \n" +
                string.Join(", ", rowStatements) + ")\n AS source (" + _allColumnsConvertedAndJoined + ") ON " + _keyColumnsConvertedAndJoined +
                (!string.IsNullOrEmpty(_valueColumnsConvertedAndJoined) ? " WHEN MATCHED THEN UPDATE SET " + _valueColumnsConvertedAndJoined : string.Empty) +
                " WHEN NOT MATCHED BY TARGET THEN INSERT (" + (UpsertKeyColumns ? _allColumnsConvertedAndJoined : _valueColumnsNames) + ") VALUES (" + (UpsertKeyColumns ? _allColumnsConvertedAndJoinedSource : _valueColumnsConvertedAndJoinedInsert) + ");";
        }
    }
}