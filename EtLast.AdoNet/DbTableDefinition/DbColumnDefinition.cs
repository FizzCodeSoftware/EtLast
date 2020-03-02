namespace FizzCode.EtLast.AdoNet
{
    public class DbColumnDefinition
    {
        public string RowColumn { get; }
        public string DbColumn { get; }

        public DbColumnDefinition(string rowColumn, string dbColumn = null)
        {
            RowColumn = rowColumn;
            DbColumn = dbColumn ?? rowColumn;
        }
    }
}