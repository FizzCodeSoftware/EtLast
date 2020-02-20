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
        public static DwhTableBuilder[] AutoValidityRange(this DwhTableBuilder[] builders, Action<AutoValidityRangeBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new AutoValidityRangeBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.MatchColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(AutoValidityRange) + " for table " + tableBuilder.Table.TableName);

                tableBuilder.AddMutatorCreator(_ => CreateAutoValidityRangeMutators(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IMutator> CreateAutoValidityRangeMutators(AutoValidityRangeBuilder builder)
        {
            var pk = builder.TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();

            var finalValueColumns = builder.CompareValueColumns
                .Where(x => builder.MatchColumns.All(kc => !string.Equals(x, kc, StringComparison.InvariantCultureIgnoreCase))
                        && (pk?.SqlColumns.All(pkc => !string.Equals(x, pkc.SqlColumn.Name, StringComparison.InvariantCultureIgnoreCase)) != false)
                        && builder.PreviousValueColumnNameMap.All(kc => !string.Equals(x, kc.Value, StringComparison.InvariantCultureIgnoreCase)))
                .ToArray();

            var equalityComparer = new ColumnBasedRowEqualityComparer()
            {
                Columns = finalValueColumns,
            };

            if (builder.MatchColumns.Length == 1)
            {
                yield return new BatchedCompareWithRowMutator(builder.TableBuilder.Table.Topic, nameof(AutoValidityRange))
                {
                    If = row => !row.IsNullOrEmpty(builder.MatchColumns[0]),
                    RightProcessCreator = rows => CreateAutoValidity_ExpandDeferredReaderProcess(builder, builder.MatchColumns[0], finalValueColumns, rows),
                    LeftKeySelector = row => row.FormatToString(builder.MatchColumns[0]),
                    RightKeySelector = row => row.FormatToString(builder.MatchColumns[0]),
                    EqualityComparer = equalityComparer,
                    NoMatchAction = new NoMatchAction(MatchMode.Custom)
                    {
                        CustomAction = (proc, row) =>
                        {
                            // this is the first version
                            row.SetStagedValue(builder.TableBuilder.ValidFromColumnName, builder.TableBuilder.DwhBuilder.DefaultValidFromDateTime);
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
                                var previousValue = match[kvp.Key];
                                row.SetStagedValue(kvp.Value, previousValue);
                            }

                            row.SetStagedValue(builder.TableBuilder.ValidFromColumnName, builder.TableBuilder.Table.Topic.Context.CreatedOnLocal);
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

                yield return new CompareWithRowMutator(builder.TableBuilder.Table.Topic, nameof(AutoValidityRange))
                {
                    RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.Table.Topic, "PreviousValueReader")
                    {
                        ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                        Sql = "SELECT " + string.Join(",", builder.MatchColumns.Concat(finalValueColumns).Select(x => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(x)))
                            + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                            + " WHERE " + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                        Parameters = parameters,
                    },
                    LeftKeySelector = row => string.Join("\0", builder.MatchColumns.Select(c => row.FormatToString(c) ?? "-")),
                    RightKeySelector = row => string.Join("\0", builder.MatchColumns.Select(c => row.FormatToString(c) ?? "-")),
                    EqualityComparer = equalityComparer,
                    NoMatchAction = new NoMatchAction(MatchMode.Custom)
                    {
                        CustomAction = (proc, row) =>
                        {
                            row.SetStagedValue(builder.TableBuilder.ValidFromColumnName, builder.TableBuilder.DwhBuilder.DefaultValidFromDateTime);
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
                                var previousValue = match[kvp.Key];
                                row.SetStagedValue(kvp.Value, previousValue);
                            }

                            row.SetStagedValue(builder.TableBuilder.ValidFromColumnName, builder.TableBuilder.Table.Topic.Context.CreatedOnLocal);
                            row.SetStagedValue(builder.TableBuilder.ValidToColumnName, builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);
                            row.ApplyStaging();
                        },
                    },
                    MatchAndEqualsAction = new MatchAction(MatchMode.Remove)
                };
            }
        }

        private static CustomSqlAdoNetDbReaderProcess CreateAutoValidity_ExpandDeferredReaderProcess(AutoValidityRangeBuilder builder, string matchColumn, string[] valueColumns, IRow[] rows)
        {
            var parameters = new Dictionary<string, object>
            {
                ["keyList"] = rows
                    .Select(row => row.FormatToString(matchColumn))
                    .Distinct()
                    .ToArray(),
            };

            if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                parameters.Add("InfiniteFuture", builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

            return new CustomSqlAdoNetDbReaderProcess(builder.TableBuilder.Table.Topic, "PreviousValueReader")
            {
                ConnectionString = builder.TableBuilder.DwhBuilder.ConnectionString,
                Sql = "SELECT " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(matchColumn)
                    + "," + string.Join(", ", valueColumns.Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c)))
                    + " FROM " + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(builder.TableBuilder.SqlTable.SchemaAndTableName.TableName, builder.TableBuilder.SqlTable.SchemaAndTableName.Schema)
                    + " WHERE "
                        + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(matchColumn) + " IN (@keyList)"
                        + " and " + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                InlineArrayParameters = true,
                Parameters = parameters,
            };
        }
    }
}