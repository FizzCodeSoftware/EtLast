namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.Linq;

    public class MsSqlServerMergeSqlStatementCreator : IAdoNetWriteToTableSqlStatementCreator
    {
        private DbTableDefinition _tableDefinition;
        private string _allDbColumns;
        private string _keyDbColumns;
        private string _updateDbColumns;
        private string _insertDbColumnsTarget;
        private string _insertDbColumnsSource;

        public void Prepare(AdoNetWriteToTableOperation operation, IProcess process, DbTableDefinition tableDefinition)
        {
            _tableDefinition = tableDefinition;

            _allDbColumns = string.Join(", ", _tableDefinition.Columns.Select(x => x.DbColumn));
            _keyDbColumns = string.Join(" AND ", _tableDefinition.Columns.Where(x => x.IsKey).Select(x => "target." + x.DbColumn + " = source." + x.DbColumn));
            _updateDbColumns = string.Join(",\n\t\t", _tableDefinition.Columns.Where(x => !x.IsKey).Select(x => x.DbColumn + " = source." + x.DbColumn));
            _insertDbColumnsTarget = string.Join(", ", _tableDefinition.Columns.Where(x => x.Insert).Select(x => x.DbColumn));
            _insertDbColumnsSource = string.Join(", ", _tableDefinition.Columns.Where(x => x.Insert).Select(x => "source." + x.DbColumn));
        }

        public string CreateRowStatement(ConnectionStringSettings settings, IRow row, AdoNetWriteToTableOperation adoNetWriteToTableOperation)
        {
            var startIndex = adoNetWriteToTableOperation.ParameterCount;
            foreach (var column in _tableDefinition.Columns)
            {
                adoNetWriteToTableOperation.CreateParameter(column, row[column.RowColumn]);
            }

            var statement = "(" + string.Join(", ", _tableDefinition.Columns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";

            if (row.Flagged)
                adoNetWriteToTableOperation.Process.Context.LogRow(adoNetWriteToTableOperation.Process, row, "sql statement generated: {SqlStatement}", statement);

            return statement;
        }

        public string CreateStatement(ConnectionStringSettings settings, List<string> rowStatements)
        {
            return "MERGE INTO " + _tableDefinition.TableName + " target USING (VALUES \n" +
                string.Join(", ", rowStatements) + "\n) AS source (" + _allDbColumns + ")\nON " + _keyDbColumns +
                (!string.IsNullOrEmpty(_updateDbColumns) ? "\nWHEN MATCHED THEN\n\tUPDATE SET\n\t\t" + _updateDbColumns : string.Empty) +
                "\nWHEN NOT MATCHED BY TARGET THEN\n\tINSERT (" + _insertDbColumnsTarget + ")\n\tVALUES (" + _insertDbColumnsSource + ");";
        }
    }
}