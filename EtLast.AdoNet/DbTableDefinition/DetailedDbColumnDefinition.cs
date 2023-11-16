namespace FizzCode.EtLast;

public sealed class DetailedDbColumnDefinition(string rowColumn, string dbColumn = null)
{
    public string RowColumn { get; } = rowColumn;
    public string DbColumn { get; } = dbColumn ?? rowColumn;
    public DbType? DbType { get; init; }

    /// <summary>
    /// Default value is true
    /// </summary>
    public bool Insert { get; init; } = true;

    /// <summary>
    /// Default value is false
    /// </summary>
    public bool IsKey { get; init; }
}
