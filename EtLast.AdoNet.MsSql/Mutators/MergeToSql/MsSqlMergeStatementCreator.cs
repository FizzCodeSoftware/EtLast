namespace FizzCode.EtLast;

public sealed class MsSqlMergeStatementCreator : IMergeToSqlStatementCreator
{
    private string _tableName;
    private DbColumn[] _allColumns;
    private string _allDbColumns;
    private string _keyDbColumns;
    private string _updateDbColumns;
    private string _insertDbColumnsTarget;
    private string _insertDbColumnsSource;

    public void Prepare(MergeToSqlMutator process, string tableName, DbColumn[] keyColumns, DbColumn[] valueColumns)
    {
        _tableName = tableName;
        _allColumns = [.. keyColumns, .. valueColumns];

        _allDbColumns = string.Join(", ", valueColumns.Select(x => x.NameInDatabase));
        _keyDbColumns = string.Join(" AND ", keyColumns.Select(x => "target." + x.NameInDatabase + " = source." + x.NameInDatabase));
        _updateDbColumns = string.Join(",\n\t\t", valueColumns.Select(x => x.NameInDatabase + " = source." + x.NameInDatabase));
        _insertDbColumnsTarget = string.Join(", ", _allColumns.Where(x => x.Insert).Select(x => x.NameInDatabase));
        _insertDbColumnsSource = string.Join(", ", _allColumns.Where(x => x.Insert).Select(x => "source." + x.NameInDatabase));
    }

    public string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, MergeToSqlMutator process)
    {
        var startIndex = process.ParameterCount;
        foreach (var column in _allColumns)
            process.CreateParameter(column.DbType, row[column.RowColumn]);

        var statement = "(" + string.Join(", ", _allColumns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";
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