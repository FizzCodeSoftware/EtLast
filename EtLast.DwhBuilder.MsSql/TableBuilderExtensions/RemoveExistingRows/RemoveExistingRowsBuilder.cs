namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Linq;
    using FizzCode.LightWeight.RelationalModel;

    public class RemoveExistingRowsBuilder
    {
        internal DwhTableBuilder TableBuilder { get; }
        internal MatchAction MatchButDifferentAction { get; private set; }
        internal RelationalColumn[] MatchColumns { get; private set; }
        internal string[] MatchColumnNames { get; private set; }
        internal RelationalColumn[] CompareValueColumns { get; private set; }

        internal RemoveExistingRowsBuilder(DwhTableBuilder tableBuilder)
        {
            TableBuilder = tableBuilder;
        }

        public RemoveExistingRowsBuilder AutoValidityIfValueChanged()
        {
            MatchButDifferentAction = new MatchAction(MatchMode.Custom)
            {
                CustomAction = (row, match) =>
                {
                    row.SetStagedValue(TableBuilder.ValidFromColumn.Name, TableBuilder.DwhBuilder.EtlRunIdAsDateTimeOffset.Value);
                    row.SetStagedValue(TableBuilder.ValidToColumnName, TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);
                    row.ApplyStaging();
                },
            };

            return this;
        }

        public RemoveExistingRowsBuilder MatchByPrimaryKey()
        {
            if (TableBuilder.Table.PrimaryKeyColumns.Count == 0)
                throw new InvalidDwhBuilderParameterException<DwhTableBuilder>(TableBuilder.DwhBuilder, nameof(MatchByPrimaryKey), TableBuilder.Table.SchemaAndName, "can't use " + nameof(MatchByPrimaryKey) + " on a table without primary key: " + TableBuilder.Table.SchemaAndName);

            MatchColumns = TableBuilder.Table.PrimaryKeyColumns.ToArray();
            MatchColumnNames = MatchColumns.Select(x => x.Name).ToArray();
            return this;
        }

        public RemoveExistingRowsBuilder MatchBySpecificColumns(params RelationalColumn[] matchColumns)
        {
            MatchColumns = matchColumns;
            MatchColumnNames = MatchColumns.Select(x => x.Name).ToArray();
            return this;
        }

        public RemoveExistingRowsBuilder MatchBySpecificColumns(params string[] matchColumns)
        {
            return MatchBySpecificColumns(matchColumns
                .Select(x => TableBuilder.Table[x])
                .ToArray());
        }

        public RemoveExistingRowsBuilder MatchByAllColumnsExceptPk()
        {
            MatchColumns = TableBuilder.Table.Columns.Where(x => !x.IsPrimaryKey).ToArray();
            MatchColumnNames = MatchColumns.Select(x => x.Name).ToArray();
            return this;
        }

        public RemoveExistingRowsBuilder CompareAllColumnsAndValidity()
        {
            CompareValueColumns = TableBuilder.Table.Columns
                .Where(x => !x.GetUsedByEtlRunInfo() && !x.GetRecordTimestampIndicator())
                .ToArray();

            // key columns will be excluded from the value column list later

            return this;
        }

        public RemoveExistingRowsBuilder CompareAllColumnsButValidity()
        {
            CompareValueColumns = TableBuilder.Table.Columns
                .Where(x => !x.GetUsedByEtlRunInfo() && !x.GetRecordTimestampIndicator()
                    && x != TableBuilder.ValidFromColumn
                    && !string.Equals(x.Name, TableBuilder.ValidToColumnName, StringComparison.InvariantCultureIgnoreCase))
                .ToArray();

            // key columns will be excluded from the value column list later

            return this;
        }

        public RemoveExistingRowsBuilder CompareSpecificColumns(params string[] valueColumns)
        {
            return CompareSpecificColumns(valueColumns
               .Select(x => TableBuilder.Table[x])
               .ToArray());
        }

        public RemoveExistingRowsBuilder CompareSpecificColumns(params RelationalColumn[] valueColumns)
        {
            CompareValueColumns = valueColumns;
            return this;
        }
    }
}