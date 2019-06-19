namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Configuration;

    public interface IAdoNetWriteToTableSqlStatementCreator
    {
        void Prepare(AdoNetWriteToTableOperation operation, IProcess process, DbTableDefinition tableDefinition);
        string CreateRowStatement(ConnectionStringSettings settings, IRow row, AdoNetWriteToTableOperation operation);
        string CreateStatement(ConnectionStringSettings settings, List<string> rowStatements);
    }
}