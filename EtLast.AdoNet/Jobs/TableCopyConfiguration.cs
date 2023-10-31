namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public sealed class TableCopyConfiguration
{
    [ProcessParameterNullException]
    public required string SourceTableName { get; init; }

    [ProcessParameterNullException]
    public required string TargetTableName { get; init; }

    /// <summary>
    /// Optional. In case of NULL all columns will be copied to the target table.
    /// </summary>
    public Dictionary<string, string> Columns { get; init; }

    public override string ToString()
    {
        return SourceTableName + "->" + TargetTableName + (Columns != null
            ? ": " + string.Join(',', Columns.Select(x => x.Key + (x.Value != null && x.Key != x.Value ? "->" + x.Value : "")))
            : "");
    }
}