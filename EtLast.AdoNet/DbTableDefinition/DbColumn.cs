namespace FizzCode.EtLast;

public sealed class DbColumn(string rowColumn, string nameOverrideInDatabase = null)
{
    public string RowColumn { get; } = rowColumn;
    public string NameInDatabase { get; } = nameOverrideInDatabase ?? rowColumn;
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