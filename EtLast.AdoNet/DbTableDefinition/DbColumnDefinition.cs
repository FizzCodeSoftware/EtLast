namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;

    [DebuggerDisplay("{RowColumn} -> {DbColumn}")]
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