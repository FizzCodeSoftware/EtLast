namespace FizzCode.EtLast;

public sealed class TableCopyConfiguration
{
    public string SourceTableName { get; init; }
    public string TargetTableName { get; init; }

    /// <summary>
    /// Optional. In case of NULL all columns will be copied to the target table.
    /// </summary>
    public Dictionary<string, string> Columns { get; init; }
}
