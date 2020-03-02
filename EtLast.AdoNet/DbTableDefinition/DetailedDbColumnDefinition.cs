namespace FizzCode.EtLast.AdoNet
{
    using System.Data;

    public class DetailedDbColumnDefinition
    {
        public string RowColumn { get; }
        public string DbColumn { get; }
        public DbType? DbType { get; set; }

        /// <summary>
        /// Default value is true
        /// </summary>
        public bool Insert { get; set; } = true;

        /// <summary>
        /// Default value is false
        /// </summary>
        public bool IsKey { get; set; }

        public DetailedDbColumnDefinition(string rowColumn, string dbColumn = null)
        {
            RowColumn = rowColumn;
            DbColumn = dbColumn ?? rowColumn;
        }
    }
}