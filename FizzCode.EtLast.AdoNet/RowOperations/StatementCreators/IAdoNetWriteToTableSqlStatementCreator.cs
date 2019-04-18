namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Configuration;

    public interface IAdoNetWriteToTableSqlStatementCreator
    {
        string TableName { get; set; }
        IEnumerable<string> AllColumns { get; }

        void Prepare(AdoNetWriteToTableOperation operation, IProcess process);
        string CreateRowStatement(ConnectionStringSettings settings, IRow row, AdoNetWriteToTableOperation op);
        string CreateStatement(ConnectionStringSettings settings, List<string> rowStatements);
    }
}
