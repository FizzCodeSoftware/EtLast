namespace FizzCode.EtLast.AdoNet
{
    public class DbTableDefinition
    {
        public string TableName { get; init; }
        public DbColumnDefinition[] Columns { get; init; }
    }
}