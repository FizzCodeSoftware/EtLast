namespace FizzCode.EtLast;

public sealed class GenericInsertSqlStatementCreator : IWriteToSqlStatementCreator
{
    private string _tableName;
    private DbColumn[] _columns;
    private string _columnNamesConcat;

    public void Prepare(WriteToSqlMutator process, string tableName, DbColumn[] columns)
    {
        _tableName = tableName;
        _columns = columns.Where(x => x.Insert).ToArray();
        _columnNamesConcat = string.Join(", ", _columns.Select(x => x.NameInDatabase));
    }

    public string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, WriteToSqlMutator operation)
    {
        var startIndex = operation.ParameterCount;
        foreach (var column in _columns)
            operation.CreateParameter(column, row[column.RowColumn]);

        var statement = "(" + string.Join(", ", _columns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";
        return statement;
    }

    public string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements)
    {
        return "INSERT INTO " + _tableName + " (" + _columnNamesConcat + ") VALUES \n" + string.Join(",\n", rowStatements) + ";";
    }
}
