namespace FizzCode.EtLast;

public sealed class GenericInsertSqlStatementCreator : IInsertToSqlStatementCreator
{
    private string _tableName;
    private DbColumn[] _columns;
    private string _columnNamesConcat;

    public void Prepare(InsertToSqlMutator process, string tableName, DbColumn[] columns)
    {
        _tableName = tableName;
        _columns = columns.Where(x => x.Insert).ToArray();
        _columnNamesConcat = string.Join(", ", _columns.Select(x => x.NameInDatabase));
    }

    public string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, InsertToSqlMutator process)
    {
        var startIndex = process.ParameterCount;
        foreach (var column in _columns)
            process.CreateParameter(column.DbType, row[column.RowColumn]);

        var statement = "(" + string.Join(", ", _columns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";
        return statement;
    }

    public string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements)
    {
        return "INSERT INTO " + _tableName + " (" + _columnNamesConcat + ") VALUES \n" + string.Join(",\n", rowStatements) + ";";
    }
}
