namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] AutoValidityRange(this DwhTableBuilder[] builders, Action<AutoValidityRangeBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new AutoValidityRangeBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.MatchColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(AutoValidityRange) + " for table " + tableBuilder.ResilientTable.TableName);

                tableBuilder.AddMutatorCreator(_ => CreateAutoValidityRangeMutators(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IMutator> CreateAutoValidityRangeMutators(AutoValidityRangeBuilder builder)
        {
            var finalValueColumns = builder.CompareValueColumns
                .Where(x => !builder.MatchColumns.Contains(x)
                        && !x.IsPrimaryKey
                        && !builder.PreviousValueColumnNameMap.ContainsValue(x))
                .ToArray();

            var equalityComparer = new ColumnBasedRowEqualityComparer()
            {
                Columns = finalValueColumns.Select(x => x.Name).ToArray(),
            };

            if (builder.MatchColumns.Length == 1)
            {
                yield return new BatchedCompareWithRowMutator(builder.TableBuilder.ResilientTable.Topic, nameof(AutoValidityRange))
                {
                    If = row => !row.IsNullOrEmpty(builder.MatchColumns[0].Name),
                    LookupBuilder = new FilteredRowLookupBuilder()
                    {
                        ProcessCreator = filterRows => CreateAutoValidity_ExpandDeferredReaderProcess(builder, builder.MatchColumns[0], finalValueColumns, filterRows),
                        KeyGenerator = row => row.GenerateKey(builder.MatchColumns[0].Name),
                    },
                    RowKeyGenerator = row => row.GenerateKey(builder.MatchColumns[0].Name),
                    EqualityComparer = equalityComparer,
                    NoMatchAction = new NoMatchAction(MatchMode.Custom)
                    {
                        CustomAction = (proc, row) =>
                        {
                            // this is the first version
                            row.SetStagedValue(builder.TableBuilder.ValidFromColumn.Name, builder.TableBuilder.DwhBuilder.DefaultValidFromDateTime);
                            row.SetStagedValue(builder.TableBuilder.ValidToColumnName, builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);
                            row.ApplyStaging();
                        }
                    },
                    MatchButDifferentAction = new MatchAction(MatchMode.Custom)
                    {
                        CustomAction = (proc, row, match) =>
                        {
                            foreach (var kvp in builder.PreviousValueColumnNameMap)
                            {
                                var previousValue = match[kvp.Key.Name];
                                row.SetStagedValue(kvp.Value.Name, previousValue);
                            }

                            row.SetStagedValue(builder.TableBuilder.ValidFromColumn.Name, builder.TableBuilder.ResilientTable.Topic.Context.CreatedOnLocal);
                            row.SetStagedValue(builder.TableBuilder.ValidToColumnName, builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);
                            row.ApplyStaging();
                        },
                    },
                    MatchAndEqualsAction = new MatchAction(MatchMode.Remove)
                };
            }
            else
            {
                var parameters = new Dictionary<string, object>();
                if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                    parameters.Add("InfiniteFuture", builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

                yield return new CompareWithRowMutator(builder.TableBuilder.ResilientTable.Topic, nameof(AutoValidityRange))
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = new CustomSqlAdoNetDbReader(builder.TableBuilder.ResilientTable.Topic, "PreviousValueReader")
                        {
                            ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                            MainTableName = builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                            Sql = "SELECT " + string.Join(",", builder.MatchColumns.Concat(finalValueColumns).Select(x => x.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)))
                                + " FROM " + builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString)
                                + " WHERE " + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                            Parameters = parameters,
                        },
                        KeyGenerator = row => row.GenerateKey(builder.MatchColumnNames),
                    },
                    RowKeyGenerator = row => row.GenerateKey(builder.MatchColumnNames),
                    EqualityComparer = equalityComparer,
                    NoMatchAction = new NoMatchAction(MatchMode.Custom)
                    {
                        CustomAction = (proc, row) =>
                        {
                            row.SetStagedValue(builder.TableBuilder.ValidFromColumn.Name, builder.TableBuilder.DwhBuilder.DefaultValidFromDateTime);
                            row.SetStagedValue(builder.TableBuilder.ValidToColumnName, builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);
                            row.ApplyStaging();
                        }
                    },
                    MatchButDifferentAction = new MatchAction(MatchMode.Custom)
                    {
                        CustomAction = (proc, row, match) =>
                        {
                            foreach (var kvp in builder.PreviousValueColumnNameMap)
                            {
                                var previousValue = match[kvp.Key.Name];
                                row.SetStagedValue(kvp.Value.Name, previousValue);
                            }

                            row.SetStagedValue(builder.TableBuilder.ValidFromColumn.Name, builder.TableBuilder.ResilientTable.Topic.Context.CreatedOnLocal);
                            row.SetStagedValue(builder.TableBuilder.ValidToColumnName, builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);
                            row.ApplyStaging();
                        },
                    },
                    MatchAndEqualsAction = new MatchAction(MatchMode.Remove)
                };
            }
        }

        private static CustomSqlAdoNetDbReader CreateAutoValidity_ExpandDeferredReaderProcess(AutoValidityRangeBuilder builder, RelationalColumn matchColumn, RelationalColumn[] valueColumns, IReadOnlySlimRow[] rows)
        {
            var parameters = new Dictionary<string, object>
            {
                ["keyList"] = rows
                    .Select(row => row.FormatToString(matchColumn.Name))
                    .Distinct()
                    .ToArray(),
            };

            if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                parameters.Add("InfiniteFuture", builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

            return new CustomSqlAdoNetDbReader(builder.TableBuilder.ResilientTable.Topic, "PreviousValueReader")
            {
                ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                MainTableName = builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString),
                Sql = "SELECT " + matchColumn.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)
                    + "," + string.Join(", ", valueColumns.Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)))
                    + " FROM " + builder.TableBuilder.Table.EscapedName(builder.TableBuilder.DwhBuilder.ConnectionString)
                    + " WHERE "
                        + matchColumn.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString) + " IN (@keyList)"
                        + " and " + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                InlineArrayParameters = true,
                Parameters = parameters,
            };
        }
    }
}