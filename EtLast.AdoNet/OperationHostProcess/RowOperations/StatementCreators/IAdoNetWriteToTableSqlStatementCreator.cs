namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using FizzCode.DbTools.Configuration;

    public interface IAdoNetWriteToTableSqlStatementCreator
    {
        void Prepare(WriteToTableOperation operation, IProcess process, DetailedDbTableDefinition tableDefinition);
        string CreateRowStatement(ConnectionStringWithProvider connectionString, IRow row, WriteToTableOperation operation);
        string CreateStatement(ConnectionStringWithProvider connectionString, List<string> rowStatements);
    }
}