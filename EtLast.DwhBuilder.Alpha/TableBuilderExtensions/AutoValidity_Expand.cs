namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] AutoValidity_Expand(this DwhTableBuilder[] builders, string[] keyColumns, string[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(builder => CreateAutoValidity_Expand(builder, keyColumns, valueColumns, customRowEqualityComparer));
            }

            return builders;
        }

        private static IEnumerable<IRowOperation> CreateAutoValidity_Expand(DwhTableBuilder builder, string[] keyColumns, string[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            yield return new DeferredCompareWithRowOperation()
            {
                InstanceName = nameof(AutoValidity_Expand),
                RightProcessCreator = rows => new CustomSqlAdoNetDbReaderProcess(builder.DwhBuilder.Context, "PreviousValueReader")
                {
                    ConnectionString = builder.DwhBuilder.ConnectionString,
                    Sql = "select " + string.Join(",", keyColumns.Concat(valueColumns.Select(x => x)))
                        + " from " + builder.DwhBuilder.ConnectionString.Escape(builder.SqlTable.SchemaAndTableName.TableName, builder.SqlTable.SchemaAndTableName.Schema)
                        + " where " + builder.DwhBuilder.Configuration.ValidToColumnName + "=@InfiniteFuture",
                    Parameters = new Dictionary<string, object>
                    {
                        ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                    },
                    ColumnConfiguration = keyColumns
                        .Select(x => new ReaderColumnConfiguration(x, new StringConverter())).ToList(),
                },
                LeftKeySelector = row => string.Join("\0", keyColumns.Select(c => row.FormatToString(c) ?? "-")),
                RightKeySelector = row => string.Join("\0", keyColumns.Select(c => row.FormatToString(c) ?? "-")),
                EqualityComparer = customRowEqualityComparer ??
                    new ColumnBasedRowEqualityComparer()
                    {
                        Columns = valueColumns.ToArray(),
                    },
                NoMatchAction = new NoMatchAction(MatchMode.Custom)
                {
                    CustomAction = (op, row) =>
                    {
                        // this is the first version
                        row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, builder.DwhBuilder.Configuration.InfinitePastDateTime, op);
                        row.SetValue(builder.DwhBuilder.Configuration.ValidToColumnName, builder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                    }
                },
                MatchButDifferentAction = new MatchAction(MatchMode.Custom)
                {
                    CustomAction = (op, row, match) =>
                    {
                        row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, DateTime.Now, op);
                        row.SetValue(builder.DwhBuilder.Configuration.ValidToColumnName, builder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                    },
                },
                MatchAndEqualsAction = new MatchAction(MatchMode.Remove)
            };
        }
    }
}