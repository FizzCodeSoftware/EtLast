namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;

    public class AutoValidityRangeBuilder
    {
        internal DwhTableBuilder TableBuilder { get; }
        internal string[] MatchColumns { get; private set; }
        internal string[] CompareValueColumns { get; private set; }
        internal Dictionary<string, string> PreviousValueColumnNameMap { get; } = new Dictionary<string, string>();

        internal AutoValidityRangeBuilder(DwhTableBuilder tableBuilder)
        {
            TableBuilder = tableBuilder;
        }

        public AutoValidityRangeBuilder MatchByPrimaryKey()
        {
            var pk = TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk == null)
                throw new NotSupportedException();

            MatchColumns = pk.SqlColumns
                .Select(x => x.SqlColumn.Name)
                .ToArray();

            return this;
        }

        public AutoValidityRangeBuilder MatchBySpecificColumns(params string[] matchColumns)
        {
            MatchColumns = matchColumns;
            return this;
        }

        public AutoValidityRangeBuilder MatchByAllColumnsExceptPk()
        {
            var pk = TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();

            MatchColumns = TableBuilder.SqlTable.Columns
                .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)))
                .Select(x => x.Name)
                .ToArray();

            return this;
        }

        public AutoValidityRangeBuilder UsePreviousValue(string valueVolumnName, string previousValueColumnName)
        {
            PreviousValueColumnNameMap.Add(valueVolumnName, previousValueColumnName);
            return this;
        }

        public AutoValidityRangeBuilder CompareAllColumnsAndValidity()
        {
            CompareValueColumns = TableBuilder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && !x.HasProperty<RecordTimestampIndicatorColumnProperty>())
                .Select(x => x.Name).ToArray();

            // key columns will be excluded from the value column list later

            return this;
        }

        public AutoValidityRangeBuilder CompareAllColumnsButValidity()
        {
            CompareValueColumns = TableBuilder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && !x.HasProperty<RecordTimestampIndicatorColumnProperty>()
                    && !string.Equals(x.Name, TableBuilder.ValidFromColumnName, StringComparison.InvariantCultureIgnoreCase)
                    && !string.Equals(x.Name, TableBuilder.ValidToColumnName, StringComparison.InvariantCultureIgnoreCase))
                .Select(x => x.Name).ToArray();

            // key columns will be excluded from the value column list later

            return this;
        }

        public AutoValidityRangeBuilder CompareSpecificColumns(params string[] valueColumns)
        {
            CompareValueColumns = valueColumns;
            return this;
        }
    }
}