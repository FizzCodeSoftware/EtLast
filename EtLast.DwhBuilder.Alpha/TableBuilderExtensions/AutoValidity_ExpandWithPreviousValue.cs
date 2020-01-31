namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] AutoValidity_ExpandWithPreviousValue(this DwhTableBuilder[] builders, string[] keyColumns, ColumnCopyConfiguration[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(builder => CreateAutoValidity_ExpandWithPreviousValue(builder, keyColumns, valueColumns, customRowEqualityComparer));
            }

            return builders;
        }

        private static IEnumerable<IRowOperation> CreateAutoValidity_ExpandWithPreviousValue(DwhTableBuilder builder, string[] keyColumns, ColumnCopyConfiguration[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            var rowEqualityComparer = customRowEqualityComparer ??
                new ColumnBasedRowEqualityComparer()
                {
                    Columns = valueColumns.Select(x => x.FromColumn).ToArray(),
                };

            yield return new DeferredExpandOperation()
            {
                InstanceName = nameof(AutoValidity_ExpandWithPreviousValue),
                RightProcessCreator = rows => new CustomSqlAdoNetDbReaderProcess(builder.DwhBuilder.Context, "PreviousValueReader")
                {
                    ConnectionString = builder.DwhBuilder.ConnectionString,
                    Sql = "select " + string.Join(",", keyColumns.Concat(valueColumns.Select(x => x.FromColumn)))
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
                ColumnConfiguration = valueColumns.ToList(),
                NoMatchAction = new NoMatchAction(MatchMode.Custom)
                {
                    CustomAction = (op, row) =>
                    {
                        // this is the first version
                        row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, builder.DwhBuilder.Configuration.InfinitePastDateTime, op);
                        row.SetValue(builder.DwhBuilder.Configuration.ValidToColumnName, builder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                    }
                },
                MatchCustomAction = (op, row, match) =>
                {
                    if (rowEqualityComparer.Equals(row, match))
                    {
                        // there is at least one existing version, but this is the same as the last
                        op.Process.RemoveRow(row, op);
                    }
                    else
                    {
                        // this is different than the last version
                        row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, DateTime.Now, op);
                        row.SetValue(builder.DwhBuilder.Configuration.ValidToColumnName, builder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                    }
                },
            };
        }
    }
}