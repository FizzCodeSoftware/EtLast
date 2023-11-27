namespace FizzCode.EtLast;

public interface IMergeToSqlStatementCreator
{
    void Prepare(MergeToSqlMutator process, string tableName, DbColumn[] keyColumns, DbColumn[] valueColumns);
    string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, MergeToSqlMutator process);
    string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements);
}