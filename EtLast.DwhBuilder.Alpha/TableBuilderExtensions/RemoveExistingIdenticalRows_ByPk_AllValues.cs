namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] RemoveExistingIdenticalRows_ByPk_AllValues(this DwhTableBuilder[] builders, bool setValidityWhenUpdateNeeded)
        {
            foreach (var builder in builders)
            {
                var pk = builder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
                if (pk == null || pk.SqlColumns.Count != 1)
                    throw new NotSupportedException();

                builder.AddOperationCreator(builder => CreateRemoveExistingIdenticalRows_ByPk_AllValues(builder, setValidityWhenUpdateNeeded));
            }

            return builders;
        }

        private static IEnumerable<IRowOperation> CreateRemoveExistingIdenticalRows_ByPk_AllValues(DwhTableBuilder builder, bool setValidityWhenUpdateNeeded)
        {
            var pk = builder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk == null || pk.SqlColumns.Count != 1)
                throw new NotSupportedException();

            var pkCol = pk.SqlColumns[0].SqlColumn;

            var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();

            var columnsToCompare = builder.SqlTable.Columns
                .Where(x => x.Name != pkCol.Name
                    && !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && !string.Equals(x.Name, builder.DwhBuilder.Configuration.ValidFromColumnName, StringComparison.InvariantCultureIgnoreCase)
                    && !string.Equals(x.Name, builder.DwhBuilder.Configuration.ValidToColumnName, StringComparison.InvariantCultureIgnoreCase));

            if (!string.IsNullOrEmpty(builder.DwhBuilder.Configuration.LastModifiedColumnName))
            {
                columnsToCompare = columnsToCompare
                    .Where(x => !string.Equals(x.Name, builder.DwhBuilder.Configuration.LastModifiedColumnName, StringComparison.InvariantCultureIgnoreCase));
            }

            var columnsNamesToCompare = columnsToCompare
                .Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name))
                .ToArray();

            yield return new DeferredCompareWithRowOperation()
            {
                InstanceName = nameof(CreateRemoveExistingIdenticalRows_ByPk_AllValues),
                If = row => !row.IsNullOrEmpty(pkCol.Name),
                MatchAction = new MatchAction(MatchMode.Remove),
                NotSameAction = !setValidityWhenUpdateNeeded ? null : new MatchAction(MatchMode.Custom)
                {
                    CustomAction = (op, row, match) =>
                    {
                        row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, DateTime.Now, op);
                        row.SetValue(builder.DwhBuilder.Configuration.ValidToColumnName, builder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                    },
                },
                LeftKeySelector = row => row.FormatToString(pkCol.Name),
                RightKeySelector = row => row.FormatToString(pkCol.Name),
                RightProcessCreator = rows => new AdoNetDbReaderProcess(builder.Table.Scope.Context, "ExistingRecordReader")
                {
                    ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                    TableName = builder.Table.TableName,
                    InlineArrayParameters = true,
                    CustomWhereClause = pkCol.Name + " IN (@idList)",
                    Parameters = new Dictionary<string, object>()
                    {
                        ["idList"] = rows
                            .Select(row => row.FormatToString(pkCol.Name))
                            .Distinct()
                            .ToArray(),
                    },
                },
                EqualityComparer = new ColumnBasedRowEqualityComparer()
                {
                    Columns = columnsNamesToCompare,
                }
            };
        }
    }
}