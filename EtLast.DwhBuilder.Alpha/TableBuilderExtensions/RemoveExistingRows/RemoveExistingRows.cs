namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] RemoveExistingRows(this DwhTableBuilder[] builders, Action<RemoveExistingRowsBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new RemoveExistingRowsBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.KeyColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(RemoveExistingRows) + " for table " + tableBuilder.Table.TableName);

                if (tempBuilder.CompareValueColumns == null && tempBuilder.MatchButDifferentAction != null)
                    throw new NotSupportedException("you must specify the comparable value columns of " + nameof(RemoveExistingRows) + " for table " + tableBuilder.Table.TableName + " if you specify the " + nameof(tempBuilder.MatchButDifferentAction));

                tableBuilder.AddOperationCreator(_ => CreateRemoveExistingIdenticalRowsOperations(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IRowOperation> CreateRemoveExistingIdenticalRowsOperations(RemoveExistingRowsBuilder builder)
        {
            if (builder.CompareValueColumns != null)
            {
                var finalValueColumns = builder.CompareValueColumns
                    .Where(x => builder.KeyColumns.All(kc => !string.Equals(x, kc, StringComparison.InvariantCultureIgnoreCase))).ToArray();

                var equalityComparer = new ColumnBasedRowEqualityComparer()
                {
                    Columns = finalValueColumns,
                };

                if (builder.KeyColumns.Length == 1)
                {
                    yield return new DeferredCompareWithRowOperation()
                    {
                        InstanceName = nameof(RemoveExistingRows),
                        If = row => !row.IsNullOrEmpty(builder.KeyColumns[0]),
                        EqualityComparer = equalityComparer,
                        LeftKeySelector = row => row.FormatToString(builder.KeyColumns[0]),
                        RightKeySelector = row => row.FormatToString(builder.KeyColumns[0]),
                        RightProcessCreator = rows => new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.Table.Scope.Context, "ExistingRowsReader")
                        {
                            Sql = "SELECT " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.KeyColumns[0])
                                    + "," + string.Join(",", finalValueColumns.Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                                + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                                + " WHERE " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.KeyColumns[0]) + " IN (@keyList)",
                            ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                            InlineArrayParameters = true,
                            Parameters = new Dictionary<string, object>()
                            {
                                ["keyList"] = rows
                                    .Select(row => row.FormatToString(builder.KeyColumns[0]))
                                    .Distinct()
                                    .ToArray(),
                            },
                        },
                        MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                        MatchButDifferentAction = builder.MatchButDifferentAction,
                    };
                }
                else
                {
                    yield return new CompareWithRowOperation()
                    {
                        InstanceName = nameof(RemoveExistingRows),
                        EqualityComparer = equalityComparer,
                        LeftKeySelector = row => string.Join("\0", builder.KeyColumns.Select(c => row.FormatToString(c) ?? "-")),
                        RightKeySelector = row => string.Join("\0", builder.KeyColumns.Select(c => row.FormatToString(c) ?? "-")),
                        RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.DwhBuilder.Context, "ExistingRowsReader")
                        {
                            ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                            Sql = "SELECT " + string.Join(",", builder.KeyColumns.Concat(finalValueColumns).Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                                + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                        },
                        MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                        MatchButDifferentAction = builder.MatchButDifferentAction,
                    };
                }
            }
            else if (builder.KeyColumns.Length == 1)
            {
                yield return new DeferredKeyTestOperation()
                {
                    InstanceName = nameof(RemoveExistingRows),
                    If = row => !row.IsNullOrEmpty(builder.KeyColumns[0]),
                    LeftKeySelector = row => row.FormatToString(builder.KeyColumns[0]),
                    RightKeySelector = row => row.FormatToString(builder.KeyColumns[0]),
                    RightProcessCreator = rows => new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.Table.Scope.Context, "ExistingRowsReader")
                    {
                        Sql = "SELECT " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.KeyColumns[0])
                            + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                            + " WHERE " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.KeyColumns[0]) + " IN (@keyList)",
                        ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                        InlineArrayParameters = true,
                        Parameters = new Dictionary<string, object>()
                        {
                            ["keyList"] = rows
                                .Select(row => row.FormatToString(builder.KeyColumns[0]))
                                .Distinct()
                                .ToArray(),
                        },
                    },
                    MatchAction = new MatchAction(MatchMode.Remove),
                };
            }
            else
            {
                yield return new KeyTestOperation()
                {
                    InstanceName = nameof(RemoveExistingRows),
                    LeftKeySelector = row => string.Join("\0", builder.KeyColumns.Select(c => row.FormatToString(c) ?? "-")),
                    RightKeySelector = row => string.Join("\0", builder.KeyColumns.Select(c => row.FormatToString(c) ?? "-")),
                    RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.DwhBuilder.Context, "ExistingRowsReader")
                    {
                        ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                        Sql = "SELECT " + string.Join(",", builder.KeyColumns.Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                            + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                    },
                    MatchAction = new MatchAction(MatchMode.Remove),
                };
            }
        }
    }
}