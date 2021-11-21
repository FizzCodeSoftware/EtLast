namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;
    using System.Linq;
    using FizzCode.LightWeight.AdoNet;

    [DebuggerDisplay("{RowColumn} -> {DbColumn}")]
    public sealed class DbColumnDefinition
    {
        public string RowColumn { get; }
        public string DbColumn { get; }

        public DbColumnDefinition(string rowColumn, string dbColumn)
        {
            RowColumn = rowColumn;
            DbColumn = dbColumn;
        }

        public static DbColumnDefinition[] StraightCopyAndEscape(NamedConnectionString connectionString, params string[] columnNames)
        {
            return columnNames
                .Select(col => new DbColumnDefinition(col, connectionString.Escape(col)))
                .ToArray();
        }
    }
}