namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public class MsSqlServerMergeSqlStatementCreator : IAdoNetWriteToTableSqlStatementCreator
    {
        private DbTableDefinition _tableDefinition;
        private string _allDbColumns;
        private string _keyDbColumns;
        private string _updateDbColumns;
        private string _insertDbColumnsTarget;
        private string _insertDbColumnsSource;

        public void Prepare(WriteToTableOperation operation, IProcess process, DbTableDefinition tableDefinition)
        {
            _tableDefinition = tableDefinition;

            _allDbColumns = string.Join(", ", _tableDefinition.Columns.Select(x => x.DbColumn));
            _keyDbColumns = string.Join(" AND ", _tableDefinition.Columns.Where(x => x.IsKey).Select(x => "target." + x.DbColumn + " = source." + x.DbColumn));
            _updateDbColumns = string.Join(",\n\t\t", _tableDefinition.Columns.Where(x => !x.IsKey).Select(x => x.DbColumn + " = source." + x.DbColumn));
            _insertDbColumnsTarget = string.Join(", ", _tableDefinition.Columns.Where(x => x.Insert).Select(x => x.DbColumn));
            _insertDbColumnsSource = string.Join(", ", _tableDefinition.Columns.Where(x => x.Insert).Select(x => "source." + x.DbColumn));
        }

        public string CreateRowStatement(ConnectionStringWithProvider connectionString, IRow row, WriteToTableOperation operation)
        {
            var startIndex = operation.ParameterCount;
            foreach (var column in _tableDefinition.Columns)
            {
                operation.CreateParameter(column, row[column.RowColumn]);
            }

            var statement = "(" + string.Join(", ", _tableDefinition.Columns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";

            if (row.Flagged)
                operation.Process.Context.LogRow(operation.Process, row, "SQL statement generated: {SqlStatement}", statement);

            return statement;
        }

        public string CreateStatement(ConnectionStringWithProvider connectionString, List<string> rowStatements)
        {
            return "MERGE INTO " + _tableDefinition.TableName + " target USING (VALUES \n" +
                string.Join(", ", rowStatements) + "\n) AS source (" + _allDbColumns + ")\nON " + _keyDbColumns +
                (!string.IsNullOrEmpty(_updateDbColumns) ? "\nWHEN MATCHED THEN\n\tUPDATE SET\n\t\t" + _updateDbColumns : "") +
                "\nWHEN NOT MATCHED BY TARGET THEN\n\tINSERT (" + _insertDbColumnsTarget + ")\n\tVALUES (" + _insertDbColumnsSource + ");";
        }
    }
}