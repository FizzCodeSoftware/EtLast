﻿namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public class GenericInsertSqlStatementCreator : IAdoNetWriteToTableSqlStatementCreator
    {
        private string _dbColumns;
        private DbTableDefinition _tableDefinition;
        private DbColumnDefinition[] _columns;

        public void Prepare(AdoNetWriteToTableOperation operation, IProcess process, DbTableDefinition tableDefinition)
        {
            _tableDefinition = tableDefinition;
            _columns = _tableDefinition.Columns.Where(x => x.Insert).ToArray();
            _dbColumns = string.Join(", ", _columns.Select(x => x.DbColumn));
        }

        public string CreateRowStatement(ConnectionStringWithProvider connectionString, IRow row, AdoNetWriteToTableOperation operation)
        {
            var startIndex = operation.ParameterCount;
            foreach (var column in _columns)
            {
                operation.CreateParameter(column, row[column.RowColumn]);
            }

            var statement = "(" + string.Join(", ", _columns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";

            if (row.Flagged)
                operation.Process.Context.LogRow(operation.Process, row, "SQL statement generated: {SqlStatement}", statement);

            return statement;
        }

        public string CreateStatement(ConnectionStringWithProvider connectionString, List<string> rowStatements)
        {
            return "INSERT INTO " + _tableDefinition.TableName + " (" + _dbColumns + ") VALUES \n" + string.Join(",\n", rowStatements) + ";";
        }
    }
}