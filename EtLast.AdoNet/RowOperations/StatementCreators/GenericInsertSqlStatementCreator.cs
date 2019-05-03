namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.Linq;

    public class GenericInsertSqlStatementCreator : IAdoNetWriteToTableSqlStatementCreator
    {
        public string TableName { get; set; }
        public string[] Columns { get; set; }
        public IEnumerable<string> AllColumns => Columns;

        public List<(string DbColumn, string RowColumn)> ColumnMap { get; set; }
        private Dictionary<string, string> _map;

        private string _allColumnsConvertedAndJoined;

        public void Prepare(AdoNetWriteToTableOperation operation, IProcess process)
        {
            if (string.IsNullOrEmpty(TableName)) throw new OperationParameterNullException(operation, nameof(TableName));
            if (Columns == null) throw new OperationParameterNullException(operation, nameof(Columns));
            _allColumnsConvertedAndJoined = string.Join(", ", Columns.Select(GetDbColumnName));

            if (ColumnMap != null)
            {
                _map = ColumnMap.ToDictionary(x => x.RowColumn, x => x.DbColumn);
            }
            else _map = null;
        }

        public string CreateRowStatement(ConnectionStringSettings settings, IRow row, AdoNetWriteToTableOperation op)
        {
            var startIndex = op.ParameterCount;
            foreach (var column in Columns)
            {
                op.CreateParameter(column, row[column]);
            }

            var statement = "(" + string.Join(", ", Columns.Select(x => "@" + (startIndex++).ToString("D", CultureInfo.InvariantCulture))) + ")";

            if (row.Flagged) op.Process.Context.LogRow(op.Process, row, "sql statement generated: {SqlStatement}", statement);

            return statement;
        }

        public string CreateStatement(ConnectionStringSettings settings, List<string> rowStatements)
        {
            return "INSERT INTO " + TableName + " (" + _allColumnsConvertedAndJoined + ") VALUES \n" + string.Join(",\n", rowStatements) + ";";
        }

        public string GetDbColumnName(string rowColumnName)
        {
            return (_map != null && _map.TryGetValue(rowColumnName, out string dbColumnName)) ? dbColumnName : rowColumnName;
        }
    }
}