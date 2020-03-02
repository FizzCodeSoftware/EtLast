namespace FizzCode.EtLast.AdoNet
{
    public class DetailedDbTableDefinition
    {
        public string TableName { get; set; }
        public DetailedDbColumnDefinition[] Columns { get; set; }
    }
}