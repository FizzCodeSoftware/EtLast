namespace FizzCode.EtLast;

public sealed class DetailedDbTableDefinition
{
    public required string TableName { get; set; }
    public required DetailedDbColumnDefinition[] Columns { get; set; }
}
