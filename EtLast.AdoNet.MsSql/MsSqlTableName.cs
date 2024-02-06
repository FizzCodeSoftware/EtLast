namespace FizzCode.EtLast;

public record MsSqlTableName
{
    public required string Schema { get; init; }
    public required string TableName { get; init; }
    public required string EscapedName { get; init; }
}
