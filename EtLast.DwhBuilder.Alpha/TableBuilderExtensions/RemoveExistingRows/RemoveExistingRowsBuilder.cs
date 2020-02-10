namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;

    public class RemoveExistingRowsBuilder
    {
        internal DwhTableBuilder TableBuilder { get; }
        internal MatchAction MatchButDifferentAction { get; private set; }
        internal string[] MatchColumns { get; private set; }
        internal string[] CompareValueColumns { get; private set; }

        internal RemoveExistingRowsBuilder(DwhTableBuilder tableBuilder)
        {
            TableBuilder = tableBuilder;
        }

        public RemoveExistingRowsBuilder AutoValidityIfValueChanged()
        {
            MatchButDifferentAction = new MatchAction(MatchMode.Custom)
            {
                CustomAction = (proc, row, match) =>
                {
                    row.SetStagedValue(TableBuilder.ValidFromColumnName, TableBuilder.DwhBuilder.Context.CreatedOnLocal);
                    row.SetStagedValue(TableBuilder.ValidToColumnName, TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);
                    row.ApplyStaging(proc);
                },
            };

            return this;
        }

        public RemoveExistingRowsBuilder MatchByPrimaryKey()
        {
            var pk = TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk == null)
                throw new NotSupportedException();

            MatchColumns = pk.SqlColumns
                .Select(x => x.SqlColumn.Name)
                .ToArray();

            return this;
        }

        public RemoveExistingRowsBuilder MatchBySpecificColumns(params string[] matchColumns)
        {
            MatchColumns = matchColumns;
            return this;
        }

        public RemoveExistingRowsBuilder MatchByAllColumnsExceptPk()
        {
            var pk = TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();

            MatchColumns = TableBuilder.SqlTable.Columns
                .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)))
                .Select(x => x.Name)
                .ToArray();

            return this;
        }

        public RemoveExistingRowsBuilder CompareAllColumnsAndValidity()
        {
            var columnsToCompare = TableBuilder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && !x.HasProperty<RecordTimestampIndicatorColumnProperty>());

            // key columns will be excluded from the value column list later

            CompareValueColumns = columnsToCompare.Select(x => x.Name).ToArray();
            return this;
        }

        public RemoveExistingRowsBuilder CompareAllColumnsButValidity()
        {
            var columnsToCompare = TableBuilder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && !x.HasProperty<RecordTimestampIndicatorColumnProperty>()
                    && !string.Equals(x.Name, TableBuilder.ValidFromColumnName, StringComparison.InvariantCultureIgnoreCase)
                    && !string.Equals(x.Name, TableBuilder.ValidToColumnName, StringComparison.InvariantCultureIgnoreCase));

            // key columns will be excluded from the value column list later

            CompareValueColumns = columnsToCompare.Select(x => x.Name).ToArray();
            return this;
        }

        public RemoveExistingRowsBuilder CompareSpecificColumns(params string[] valueColumns)
        {
            CompareValueColumns = valueColumns;
            return this;
        }
    }
}