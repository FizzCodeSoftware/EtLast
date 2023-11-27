namespace FizzCode.EtLast;

public interface IWriteToSqlStatementCreator
{
    void Prepare(WriteToSqlMutator process, string tableName, DbColumn[] columns);
    string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, WriteToSqlMutator operation);
    string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements);
}