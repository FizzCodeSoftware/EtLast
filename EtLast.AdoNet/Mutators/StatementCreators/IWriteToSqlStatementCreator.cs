namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.AdoNet;

    public interface IWriteToSqlStatementCreator
    {
        void Prepare(WriteToSqlMutator process, DetailedDbTableDefinition tableDefinition);
        string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, WriteToSqlMutator operation);
        string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements);
    }
}