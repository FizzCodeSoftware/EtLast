namespace FizzCode.EtLast.AdoNet
{
    public class DbTableDefinition
    {
        public string TableName { get; set; }
        public DbColumnDefinition[] Columns { get; set; }
    }
}