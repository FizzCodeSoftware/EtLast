namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.AdoNet;

    public interface IAdoNetWriteToTableSqlStatementCreator
    {
        void Prepare(WriteToTableMutator process, DetailedDbTableDefinition tableDefinition);
        string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, WriteToTableMutator operation);
        string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements);
    }
}