namespace FizzCode.EtLast;

public sealed class MsSqlMergeStatementCreator : IWriteToSqlStatementCreator
{
    private DetailedDbTableDefinition _tableDefinition;
    private string _allDbColumns;
    private string _keyDbColumns;
    private string _updateDbColumns;
    private string _insertDbColumnsTarget;
    private string _insertDbColumnsSource;

    public void Prepare(WriteToSqlMutator process, DetailedDbTableDefinition tableDefinition)
    {
        _tableDefinition = tableDefinition;

        _allDbColumns = string.Join(", ", _tableDefinition.Columns.Select(x => x.DbColumn));
        _keyDbColumns = string.Join(" AND ", _tableDefinition.Columns.Where(x => x.IsKey).Select(x => "target." + x.DbColumn + " = source." + x.DbColumn));
        _updateDbColumns = string.Join(",\n\t\t", _tableDefinition.Columns.Where(x => !x.IsKey).Select(x => x.DbColumn + " = source." + x.DbColumn));
        _insertDbColumnsTarget = string.Join(", ", _tableDefinition.Columns.Where(x => x.Insert).Select(x => x.DbColumn));
        _insertDbColumnsSource = string.Join(", ", _tableDefinition.Columns.Where(x => x.Insert).Select(x => "source." + x.DbColumn));
    }

    public string CreateRowStatement(NamedConnectionString connectionString, IReadOnlySlimRow row, WriteToSqlMutator operation)
    {
        var startIndex = operation.ParameterCount;
        foreach (var column in _tableDefinition.Columns)
        {
            operation.CreateParameter(column, row[column.RowColumn]);
        }

        var statement = "(" + string.Join(", ", _tableDefinition.Columns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";
        return statement;
    }

    public string CreateStatement(NamedConnectionString connectionString, List<string> rowStatements)
    {
        return "MERGE INTO " + _tableDefinition.TableName + " target USING (VALUES \n"
            + string.Join(", ", rowStatements) + "\n) AS source (" + _allDbColumns + ")\nON " + _keyDbColumns
            + (!string.IsNullOrEmpty(_updateDbColumns) ? "\nWHEN MATCHED THEN\n\tUPDATE SET\n\t\t" + _updateDbColumns : "")
            + "\nWHEN NOT MATCHED BY TARGET THEN\n\tINSERT (" + _insertDbColumnsTarget + ")\n\tVALUES (" + _insertDbColumnsSource + ");";
    }
}
