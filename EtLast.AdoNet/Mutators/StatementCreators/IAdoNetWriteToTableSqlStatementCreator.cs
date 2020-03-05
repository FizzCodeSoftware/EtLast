namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using FizzCode.DbTools.Configuration;

    public interface IAdoNetWriteToTableSqlStatementCreator
    {
        void Prepare(WriteToTableMutator process, DetailedDbTableDefinition tableDefinition);
        string CreateRowStatement(ConnectionStringWithProvider connectionString, IReadOnlySlimRow row, WriteToTableMutator operation);
        string CreateStatement(ConnectionStringWithProvider connectionString, List<string> rowStatements);
    }
}