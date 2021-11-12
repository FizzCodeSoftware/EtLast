namespace FizzCode.EtLast.AdoNet
{
    public sealed class DbTableDefinition
    {
        public string TableName { get; init; }
        public DbColumnDefinition[] Columns { get; init; }
    }
}