namespace FizzCode.EtLast;

public interface IInsertToSqlStatementCreator
{
    void Prepare(InsertToSqlMutator process, string tableName, DbColumn[] columns);
    string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, InsertToSqlMutator process);
    string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements);
}