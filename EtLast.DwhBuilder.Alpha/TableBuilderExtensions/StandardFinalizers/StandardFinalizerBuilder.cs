namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;

    public class StandardFinalizerBuilder
    {
        internal DwhTableBuilder TableBuilder { get; }
        internal string[] KeyColumns { get; private set; }

        internal StandardFinalizerBuilder(DwhTableBuilder tableBuilder)
        {
            TableBuilder = tableBuilder;
        }

        public StandardFinalizerBuilder UsePrimaryKey()
        {
            var pk = TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk == null)
                throw new NotSupportedException();

            KeyColumns = pk.SqlColumns.Select(x => x.SqlColumn.Name).ToArray();
            return this;
        }

        public StandardFinalizerBuilder UseSpecificKeyColumns(params string[] keyColumns)
        {
            KeyColumns = keyColumns;
            return this;
        }
    }
}