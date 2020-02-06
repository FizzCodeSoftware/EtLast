namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
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

                if (tempBuilder.MatchColumns == null)
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
                var pk = builder.TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();

                var finalValueColumns = builder.CompareValueColumns
                    .Where(x => builder.MatchColumns.All(kc => !string.Equals(x, kc, StringComparison.InvariantCultureIgnoreCase))
                        && (pk?.SqlColumns.All(pkc => !string.Equals(x, pkc.SqlColumn.Name, StringComparison.InvariantCultureIgnoreCase)) != false))
                    .ToArray();

                var equalityComparer = new ColumnBasedRowEqualityComparer()
                {
                    Columns = finalValueColumns,
                };

                if (builder.MatchColumns.Length == 1)
                {
                    yield return new DeferredCompareWithRowOperation()
                    {
                        InstanceName = nameof(RemoveExistingRows),
                        If = row => !row.IsNullOrEmpty(builder.MatchColumns[0]),
                        EqualityComparer = equalityComparer,
                        LeftKeySelector = row => row.FormatToString(builder.MatchColumns[0]),
                        RightKeySelector = row => row.FormatToString(builder.MatchColumns[0]),
                        RightProcessCreator = rows => new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.Table.Scope.Context, nameof(RemoveExistingRows) + "Reader", builder.TableBuilder.Table.Topic)
                        {
                            Sql = "SELECT " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0])
                                    + "," + string.Join(", ", finalValueColumns.Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                                + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                                + " WHERE " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0]) + " IN (@keyList)",
                            ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                            InlineArrayParameters = true,
                            Parameters = new Dictionary<string, object>()
                            {
                                ["keyList"] = rows
                                    .Select(row => row.FormatToString(builder.MatchColumns[0]))
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
                        LeftKeySelector = row => string.Join("\0", builder.MatchColumns.Select(c => row.FormatToString(c) ?? "-")),
                        RightKeySelector = row => string.Join("\0", builder.MatchColumns.Select(c => row.FormatToString(c) ?? "-")),
                        RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.DwhBuilder.Context, nameof(RemoveExistingRows) + "Reader", builder.TableBuilder.Table.Topic)
                        {
                            ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                            Sql = "SELECT " + string.Join(",", builder.MatchColumns.Concat(finalValueColumns).Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                                + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                        },
                        MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                        MatchButDifferentAction = builder.MatchButDifferentAction,
                    };
                }
            }
            else if (builder.MatchColumns.Length == 1)
            {
                yield return new DeferredKeyTestOperation()
                {
                    InstanceName = nameof(RemoveExistingRows),
                    If = row => !row.IsNullOrEmpty(builder.MatchColumns[0]),
                    LeftKeySelector = row => row.FormatToString(builder.MatchColumns[0]),
                    RightKeySelector = row => row.FormatToString(builder.MatchColumns[0]),
                    RightProcessCreator = rows => new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.Table.Scope.Context, nameof(RemoveExistingRows) + "Reader", builder.TableBuilder.Table.Topic)
                    {
                        Sql = "SELECT " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0])
                            + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                            + " WHERE " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0]) + " IN (@keyList)",
                        ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                        InlineArrayParameters = true,
                        Parameters = new Dictionary<string, object>()
                        {
                            ["keyList"] = rows
                                .Select(row => row.FormatToString(builder.MatchColumns[0]))
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
                    LeftKeySelector = row => string.Join("\0", builder.MatchColumns.Select(c => row.FormatToString(c) ?? "-")),
                    RightKeySelector = row => string.Join("\0", builder.MatchColumns.Select(c => row.FormatToString(c) ?? "-")),
                    RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.DwhBuilder.Context, nameof(RemoveExistingRows) + "Reader", builder.TableBuilder.Table.Topic)
                    {
                        ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                        Sql = "SELECT " + string.Join(",", builder.MatchColumns.Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                            + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                    },
                    MatchAction = new MatchAction(MatchMode.Remove),
                };
            }
        }
    }
}