namespace FizzCode.EtLast.DwhBuilder.MsSql
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

                if (tempBuilder.MatchColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(RemoveExistingRows) + " for table " + tableBuilder.ResilientTable.TableName);

                if (tempBuilder.CompareValueColumns == null && tempBuilder.MatchButDifferentAction != null)
                    throw new NotSupportedException("you must specify the comparable value columns of " + nameof(RemoveExistingRows) + " for table " + tableBuilder.ResilientTable.TableName + " if you specify the " + nameof(tempBuilder.MatchButDifferentAction));

                tableBuilder.AddMutatorCreator(_ => CreateRemoveExistingIdenticalRowsMutators(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IMutator> CreateRemoveExistingIdenticalRowsMutators(RemoveExistingRowsBuilder builder)
        {
            if (builder.CompareValueColumns != null)
            {
                var finalValueColumns = builder.CompareValueColumns
                    .Where(x => !builder.MatchColumns.Contains(x)
                        && !x.IsPrimaryKey)
                    .ToArray();

                var equalityComparer = new ColumnBasedRowEqualityComparer()
                {
                    Columns = finalValueColumns.Select(x => x.Name).ToArray(),
                };

                if (builder.MatchColumns.Length == 1)
                {
                    yield return new BatchedCompareWithRowMutator(builder.TableBuilder.ResilientTable.Topic, nameof(RemoveExistingRows))
                    {
                        If = row => row.HasValue(builder.MatchColumns[0].Name),
                        EqualityComparer = equalityComparer,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows => new CustomSqlAdoNetDbReader(builder.TableBuilder.ResilientTable.Topic, "ExistingRowsReader")
                            {
                                ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                                MainTableName = builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                                Sql = "SELECT " + builder.MatchColumns[0].NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)
                                        + "," + string.Join(", ", finalValueColumns.Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)))
                                    + " FROM " + builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString)
                                    + " WHERE " + builder.MatchColumns[0].NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString) + " IN (@keyList)",
                                InlineArrayParameters = true,
                                Parameters = new Dictionary<string, object>()
                                {
                                    ["keyList"] = filterRows
                                        .Select(row => row.FormatToString(builder.MatchColumns[0].Name))
                                        .Distinct()
                                        .ToArray(),
                                },
                            },
                            KeyGenerator = row => row.GenerateKey(builder.MatchColumns[0].Name),
                        },
                        RowKeyGenerator = row => row.GenerateKey(builder.MatchColumns[0].Name),
                        MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                        MatchButDifferentAction = builder.MatchButDifferentAction,
                    };
                }
                else
                {
                    yield return new CompareWithRowMutator(builder.TableBuilder.ResilientTable.Topic, nameof(RemoveExistingRows))
                    {
                        EqualityComparer = equalityComparer,
                        LookupBuilder = new RowLookupBuilder()
                        {
                            Process = new CustomSqlAdoNetDbReader(builder.TableBuilder.ResilientTable.Topic, "ExistingRowsReader")
                            {
                                ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                                MainTableName = builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                                Sql = "SELECT " + string.Join(",", builder.MatchColumns.Concat(finalValueColumns).Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)))
                                + " FROM " + builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                            },
                            KeyGenerator = row => row.GenerateKey(builder.MatchColumnNames),
                        },
                        RowKeyGenerator = row => row.GenerateKey(builder.MatchColumnNames),
                        MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                        MatchButDifferentAction = builder.MatchButDifferentAction,
                    };
                }
            }
            else if (builder.MatchColumns.Length == 1)
            {
                yield return new BatchedKeyTestMutator(builder.TableBuilder.ResilientTable.Topic, nameof(RemoveExistingRows))
                {
                    If = row => row.HasValue(builder.MatchColumns[0].Name),
                    LookupBuilder = new FilteredRowLookupBuilder()
                    {
                        ProcessCreator = filterRows => new CustomSqlAdoNetDbReader(builder.TableBuilder.ResilientTable.Topic, "ExistingRowsReader")
                        {
                            ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                            MainTableName = builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                            Sql = "SELECT " + builder.MatchColumns[0].NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)
                                + " FROM " + builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString)
                                + " WHERE " + builder.MatchColumns[0].NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString) + " IN (@keyList)",
                            InlineArrayParameters = true,
                            Parameters = new Dictionary<string, object>()
                            {
                                ["keyList"] = filterRows
                                    .Select(row => row.FormatToString(builder.MatchColumns[0].Name))
                                    .Distinct()
                                    .ToArray(),
                            },
                        },
                        KeyGenerator = row => row.GenerateKey(builder.MatchColumns[0].Name),
                    },
                    RowKeyGenerator = row => row.GenerateKey(builder.MatchColumns[0].Name),
                    MatchActionContainsMatch = false,
                    MatchAction = new MatchAction(MatchMode.Remove),
                };
            }
            else
            {
                yield return new KeyTestMutator(builder.TableBuilder.ResilientTable.Topic, nameof(RemoveExistingRows))
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = new CustomSqlAdoNetDbReader(builder.TableBuilder.ResilientTable.Topic, "ExistingRowsReader")
                        {
                            ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                            MainTableName = builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                            Sql = "SELECT " + string.Join(",", builder.MatchColumns.Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)))
                                + " FROM " + builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                        },
                        KeyGenerator = row => row.GenerateKey(builder.MatchColumnNames),
                    },
                    RowKeyGenerator = row => row.GenerateKey(builder.MatchColumnNames),
                    MatchActionContainsMatch = false,
                    MatchAction = new MatchAction(MatchMode.Remove),
                };
            }
        }
    }
}