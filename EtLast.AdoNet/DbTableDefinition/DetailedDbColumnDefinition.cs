namespace FizzCode.EtLast;

public sealed class DetailedDbColumnDefinition
{
    public string RowColumn { get; }
    public string DbColumn { get; }
    public DbType? DbType { get; init; }

    /// <summary>
    /// Default value is true
    /// </summary>
    public bool Insert { get; init; } = true;

    /// <summary>
    /// Default value is false
    /// </summary>
    public bool IsKey { get; init; }

    public DetailedDbColumnDefinition(string rowColumn, string dbColumn = null)
    {
        RowColumn = rowColumn;
        DbColumn = dbColumn ?? rowColumn;
    }
}
