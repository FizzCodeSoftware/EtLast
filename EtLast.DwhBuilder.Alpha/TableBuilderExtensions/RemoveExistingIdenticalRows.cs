namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] RemoveExistingIdenticalRows(this DwhTableBuilder[] builders, string[] keyColumns, string[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(builder => CreateRemoveExistingIdenticalRows(builder, keyColumns, valueColumns, customRowEqualityComparer));
            }

            return builders;
        }

        private static IEnumerable<IRowOperation> CreateRemoveExistingIdenticalRows(DwhTableBuilder builder, string[] keyColumns, string[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            var columnsNamesToCompare = valueColumns
                .Select(c => builder.DwhBuilder.ConnectionString.Escape(c))
                .ToArray();

            yield return new CompareWithRowOperation()
            {
                InstanceName = nameof(RemoveExistingIdenticalRows),
                RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.DwhBuilder.Context, "ExistingRowsReader")
                {
                    ConnectionString = builder.DwhBuilder.ConnectionString,
                    Sql = "select " + string.Join(",", keyColumns.Concat(valueColumns))
                        + " from " + builder.DwhBuilder.ConnectionString.Escape(builder.SqlTable.SchemaAndTableName.TableName, builder.SqlTable.SchemaAndTableName.Schema),
                    ColumnConfiguration = keyColumns
                        .Select(x => new ReaderColumnConfiguration(x, new StringConverter())).ToList(),
                },
                LeftKeySelector = row => string.Join("\0", keyColumns.Select(c => row.FormatToString(c) ?? "-")),
                RightKeySelector = row => string.Join("\0", keyColumns.Select(c => row.FormatToString(c) ?? "-")),
                EqualityComparer = customRowEqualityComparer ??
                    new ColumnBasedRowEqualityComparer()
                    {
                        Columns = columnsNamesToCompare,
                    },
                MatchAction = new MatchAction(MatchMode.Remove),
            };
        }
    }
}