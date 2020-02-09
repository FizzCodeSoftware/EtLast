namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;

    public class SimpleMergeFinalizerBuilder
    {
        internal DwhTableBuilder TableBuilder { get; }
        internal string[] KeyColumns { get; private set; }

        internal SimpleMergeFinalizerBuilder(DwhTableBuilder tableBuilder)
        {
            TableBuilder = tableBuilder;
        }

        public SimpleMergeFinalizerBuilder UsePrimaryKey()
        {
            var pk = TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk == null)
                throw new NotSupportedException();

            KeyColumns = pk.SqlColumns.Select(x => x.SqlColumn.Name).ToArray();
            return this;
        }

        public SimpleMergeFinalizerBuilder UseSpecificKeyColumns(params string[] keyColumns)
        {
            KeyColumns = keyColumns;
            return this;
        }
    }
}