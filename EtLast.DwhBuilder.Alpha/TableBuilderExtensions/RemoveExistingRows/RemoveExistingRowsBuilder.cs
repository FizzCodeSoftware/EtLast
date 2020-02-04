namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;

    public class RemoveExistingRowsBuilder
    {
        internal DwhTableBuilder TableBuilder { get; }
        internal MatchAction MatchButDifferentAction { get; set; }
        internal string[] KeyColumns { get; set; }
        internal string[] CompareValueColumns { get; set; }

        internal RemoveExistingRowsBuilder(DwhTableBuilder tableBuilder)
        {
            TableBuilder = tableBuilder;
        }

        public RemoveExistingRowsBuilder AutoValidityIfValueChanged()
        {
            MatchButDifferentAction = new MatchAction(MatchMode.Custom)
            {
                CustomAction = (op, row, match) =>
                {
                    row.SetValue(TableBuilder.DwhBuilder.Configuration.ValidFromColumnName, TableBuilder.DwhBuilder.Context.CreatedOnLocal, op);
                    row.SetValue(TableBuilder.DwhBuilder.Configuration.ValidToColumnName, TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                },
            };

            return this;
        }

        public RemoveExistingRowsBuilder UsePrimaryKey()
        {
            var pk = TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk == null)
                throw new NotSupportedException();

            KeyColumns = pk.SqlColumns.Select(x => x.SqlColumn.Name).ToArray();
            return this;
        }

        public RemoveExistingRowsBuilder UseSpecificKeyColumns(params string[] keyColumns)
        {
            KeyColumns = keyColumns;
            return this;
        }

        public RemoveExistingRowsBuilder CompareAllValueColumns()
        {
            var columnsToCompare = TableBuilder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && !string.Equals(x.Name, TableBuilder.DwhBuilder.Configuration.ValidFromColumnName, StringComparison.InvariantCultureIgnoreCase)
                    && !string.Equals(x.Name, TableBuilder.DwhBuilder.Configuration.ValidToColumnName, StringComparison.InvariantCultureIgnoreCase));

            // key columns will be excluded from the value column list later

            if (!string.IsNullOrEmpty(TableBuilder.DwhBuilder.Configuration.LastModifiedColumnName))
            {
                columnsToCompare = columnsToCompare
                    .Where(x => !string.Equals(x.Name, TableBuilder.DwhBuilder.Configuration.LastModifiedColumnName, StringComparison.InvariantCultureIgnoreCase));
            }

            CompareValueColumns = columnsToCompare.Select(x => x.Name).ToArray();
            return this;
        }

        public RemoveExistingRowsBuilder CompareSpecificValueColumns(params string[] valueColumns)
        {
            CompareValueColumns = valueColumns;
            return this;
        }
    }
}