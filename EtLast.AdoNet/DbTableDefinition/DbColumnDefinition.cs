namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

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

        public static DbColumnDefinition[] StraightCopyAndEscape(ConnectionStringWithProvider connectionString, params string[] columnNames)
        {
            return columnNames
                .Select(col => new DbColumnDefinition(col, connectionString.Escape(col)))
                .ToArray();
        }
    }
}