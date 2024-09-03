namespace FizzCode.EtLast;

public interface IInsertToSqlStatementCreator
{
    void Prepare(InsertToSqlMutator process, string tableName, DbColumn[] columns);
    string CreateRowStatement(IAdoNetSqlConnectionString connectionString, IReadOnlySlimRow row, InsertToSqlMutator process);
    string CreateStatement(IAdoNetSqlConnectionString connectionString, List<string> rowStatements);
}