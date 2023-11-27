namespace FizzCode.EtLast;

public sealed class MsSqlMergeStatementCreator : IWriteToSqlStatementCreator
{
    private string _tableName;
    private DbColumn[] _columns;
    private string _allDbColumns;
    private string _keyDbColumns;
    private string _updateDbColumns;
    private string _insertDbColumnsTarget;
    private string _insertDbColumnsSource;

    public void Prepare(WriteToSqlMutator process, string tableName, DbColumn[] columns)
    {
        _tableName = tableName;
        _columns = columns;

        _allDbColumns = string.Join(", ", columns.Select(x => x.NameInDatabase));
        _keyDbColumns = string.Join(" AND ", columns.Where(x => x.IsKey).Select(x => "target." + x.NameInDatabase + " = source." + x.NameInDatabase));
        _updateDbColumns = string.Join(",\n\t\t", columns.Where(x => !x.IsKey).Select(x => x.NameInDatabase + " = source." + x.NameInDatabase));
        _insertDbColumnsTarget = string.Join(", ", columns.Where(x => x.Insert).Select(x => x.NameInDatabase));
        _insertDbColumnsSource = string.Join(", ", columns.Where(x => x.Insert).Select(x => "source." + x.NameInDatabase));
    }

    public string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, WriteToSqlMutator operation)
    {
        var startIndex = operation.ParameterCount;
        foreach (var column in _columns)
        {
            operation.CreateParameter(column, row[column.RowColumn]);
        }

        var statement = "(" + string.Join(", ", _columns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";
        return statement;
    }

    public string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements)
    {
        return "MERGE INTO " + _tableName + " target USING (VALUES \n"
            + string.Join(", ", rowStatements) + "\n) AS source (" + _allDbColumns + ")\nON " + _keyDbColumns
            + (!string.IsNullOrEmpty(_updateDbColumns) ? "\nWHEN MATCHED THEN\n\tUPDATE SET\n\t\t" + _updateDbColumns : "")
            + "\nWHEN NOT MATCHED BY TARGET THEN\n\tINSERT (" + _insertDbColumnsTarget + ")\n\tVALUES (" + _insertDbColumnsSource + ");";
    }
}
