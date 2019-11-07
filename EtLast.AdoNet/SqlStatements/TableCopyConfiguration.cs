namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public class TableCopyConfiguration
    {
        public string SourceTableName { get; set; }
        public string TargetTableName { get; set; }

        /// <summary>
        /// Optional. In case of NULL all columns will be copied to the target table.
        /// </summary>
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
    }
}