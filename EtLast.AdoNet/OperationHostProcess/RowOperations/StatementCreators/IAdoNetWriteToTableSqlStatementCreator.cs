namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using FizzCode.DbTools.Configuration;

    public interface IAdoNetWriteToTableSqlStatementCreator
    {
        void Prepare(AdoNetWriteToTableOperation operation, IProcess process, DbTableDefinition tableDefinition);
        string CreateRowStatement(ConnectionStringWithProvider connectionString, IRow row, AdoNetWriteToTableOperation operation);
        string CreateStatement(ConnectionStringWithProvider connectionString, List<string> rowStatements);
    }
}