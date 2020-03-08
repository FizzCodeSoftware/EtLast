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

                tableBuilder.AddMutatorCreator(_ => CreateRemoveExistingIdenticalRowsMutators(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IMutator> CreateRemoveExistingIdenticalRowsMutators(RemoveExistingRowsBuilder builder)
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
                    yield return new BatchedCompareWithRowMutator(builder.TableBuilder.Table.Topic, nameof(RemoveExistingRows))
                    {
                        If = row => !row.IsNullOrEmpty(builder.MatchColumns[0]),
                        EqualityComparer = equalityComparer,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows => new CustomSqlAdoNetDbReader(builder.TableBuilder.Table.Topic, "ExistingRowsReader")
                            {
                                ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                                MainTableName = builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                                Sql = "SELECT " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0])
                                        + "," + string.Join(", ", finalValueColumns.Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                                    + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                                    + " WHERE " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0]) + " IN (@keyList)",
                                InlineArrayParameters = true,
                                Parameters = new Dictionary<string, object>()
                                {
                                    ["keyList"] = filterRows
                                        .Select(row => row.FormatToString(builder.MatchColumns[0]))
                                        .Distinct()
                                        .ToArray(),
                                },
                            },
                            KeyGenerator = row => row.GenerateKey(builder.MatchColumns[0]),
                        },
                        RowKeyGenerator = row => row.GenerateKey(builder.MatchColumns[0]),
                        MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                        MatchButDifferentAction = builder.MatchButDifferentAction,
                    };
                }
                else
                {
                    yield return new CompareWithRowMutator(builder.TableBuilder.Table.Topic, nameof(RemoveExistingRows))
                    {
                        EqualityComparer = equalityComparer,
                        LookupBuilder = new RowLookupBuilder()
                        {
                            Process = new CustomSqlAdoNetDbReader(builder.TableBuilder.Table.Topic, "ExistingRowsReader")
                            {
                                ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                                MainTableName = builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                                Sql = "SELECT " + string.Join(",", builder.MatchColumns.Concat(finalValueColumns).Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                                + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                            },
                            KeyGenerator = row => row.GenerateKey(builder.MatchColumns),
                        },
                        RowKeyGenerator = row => row.GenerateKey(builder.MatchColumns),
                        MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                        MatchButDifferentAction = builder.MatchButDifferentAction,
                    };
                }
            }
            else if (builder.MatchColumns.Length == 1)
            {
                yield return new BatchedKeyTestMutator(builder.TableBuilder.Table.Topic, nameof(RemoveExistingRows))
                {
                    If = row => !row.IsNullOrEmpty(builder.MatchColumns[0]),
                    LookupBuilder = new FilteredRowLookupBuilder()
                    {
                        ProcessCreator = filterRows => new CustomSqlAdoNetDbReader(builder.TableBuilder.Table.Topic, "ExistingRowsReader")
                        {
                            ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                            MainTableName = builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                            Sql = "SELECT " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0])
                                + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                                + " WHERE " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.MatchColumns[0]) + " IN (@keyList)",
                            InlineArrayParameters = true,
                            Parameters = new Dictionary<string, object>()
                            {
                                ["keyList"] = filterRows
                                    .Select(row => row.FormatToString(builder.MatchColumns[0]))
                                    .Distinct()
                                    .ToArray(),
                            },
                        },
                        KeyGenerator = row => row.GenerateKey(builder.MatchColumns[0]),
                    },
                    RowKeyGenerator = row => row.GenerateKey(builder.MatchColumns[0]),
                    MatchActionContainsMatch = false,
                    MatchAction = new MatchAction(MatchMode.Remove),
                };
            }
            else
            {
                yield return new KeyTestMutator(builder.TableBuilder.Table.Topic, nameof(RemoveExistingRows))
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = new CustomSqlAdoNetDbReader(builder.TableBuilder.Table.Topic, "ExistingRowsReader")
                        {
                            ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                            MainTableName = builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                            Sql = "SELECT " + string.Join(",", builder.MatchColumns.Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                                + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema),
                        },
                        KeyGenerator = row => row.GenerateKey(builder.MatchColumns),
                    },
                    RowKeyGenerator = row => row.GenerateKey(builder.MatchColumns),
                    MatchActionContainsMatch = false,
                    MatchAction = new MatchAction(MatchMode.Remove),
                };
            }
        }
    }
}