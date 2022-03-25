namespace FizzCode.EtLast;

public sealed class DetailedDbTableDefinition
{
    public string TableName { get; set; }
    public DetailedDbColumnDefinition[] Columns { get; set; }
}
