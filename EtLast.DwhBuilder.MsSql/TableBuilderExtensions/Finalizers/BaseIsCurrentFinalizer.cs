namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] BaseIsCurrentFinalizer(this DwhTableBuilder[] builders, Action<KeyBasedFinalizerBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new KeyBasedFinalizerBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.MatchColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(BaseIsCurrentFinalizer) + " for table " + tableBuilder.ResilientTable.TableName);

                tableBuilder.AddFinalizerCreator(_ => CreateBaseIsCurrentFinalizer(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateBaseIsCurrentFinalizer(KeyBasedFinalizerBuilder builder)
        {
            var hasHistoryTable = builder.TableBuilder.Table.GetHasHistoryTable();
            var currentEtlRunId = builder.TableBuilder.ResilientTable.Topic.Context.AdditionalData.GetAs("CurrentEtlRunId", DateTime.UtcNow);

            var mergeIntoBaseColumns = builder.TableBuilder.Table.Columns
                .Where(x => !x.GetUsedByEtlRunInfo());

            if (builder.TableBuilder.Table.AnyPrimaryKeyColumnIsIdentity)
            {
                mergeIntoBaseColumns = mergeIntoBaseColumns
                    .Where(x => !x.IsPrimaryKey);
            }

            var mergeIntoBaseColumnNames = mergeIntoBaseColumns
                .Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString))
                .ToArray();

            var columnNamesToMatch = builder.MatchColumns
                .Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString))
                .ToArray();

            var parameters1 = new Dictionary<string, object>();
            if (builder.TableBuilder.EtlRunInsertColumnNameEscaped != null
                || builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null
                || builder.TableBuilder.EtlRunFromColumnNameEscaped != null)
            {
                parameters1.Add("EtlRunId", currentEtlRunId);
            }

            var columnNamesToUpdate = builder.TableBuilder.Table.Columns
                .Where(x => !x.GetUsedByEtlRunInfo()
                    && !x.IsPrimaryKey
                    && !builder.MatchColumns.Contains(x))
                .Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString))
                .ToArray();

            yield return new CustomMsSqlMergeStatement(builder.TableBuilder.ResilientTable.Topic, "MergeIntoBase")
            {
                ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                TargetTableName = builder.TableBuilder.ResilientTable.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x)),
                WhenMatchedAction = columnNamesToUpdate.Length > 0 || (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null)
                    ? "UPDATE SET "
                        + string.Join(", ", columnNamesToUpdate.Select(c => "t." + c + "=s." + c))
                        + (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null
                            ? (columnNamesToUpdate.Length > 0 ? ", " : "") + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + "=@EtlRunId"
                            : "")
                    : null,
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", mergeIntoBaseColumnNames)
                    + (builder.TableBuilder.EtlRunInsertColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlRunInsertColumnNameEscaped : "")
                    + (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlRunUpdateColumnNameEscaped : "")
                    + ") VALUES ("
                        + string.Join(", ", mergeIntoBaseColumnNames.Select(c => "s." + c))
                        + (builder.TableBuilder.EtlRunInsertColumnNameEscaped != null ? ", @EtlRunId" : "")
                        + (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null ? ", @EtlRunId" : "")
                    + ")",
                Parameters = parameters1,
            };

            if (hasHistoryTable)
            {
                var histTableName = builder.TableBuilder.DwhBuilder.GetEscapedHistTableName(builder.TableBuilder.Table);

                var parameters2 = new Dictionary<string, object>();
                if (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null || builder.TableBuilder.EtlRunToColumnNameEscaped != null)
                    parameters2.Add("EtlRunId", currentEtlRunId);

                if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                    parameters2.Add("InfiniteFuture", builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

                yield return new CustomMsSqlMergeStatement(builder.TableBuilder.ResilientTable.Topic, "CloseOpenEndedHistoryRecords")
                {
                    ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                    CommandTimeout = 60 * 60,
                    SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                    TargetTableName = histTableName,
                    SourceTableAlias = "s",
                    TargetTableAlias = "t",
                    OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x))
                        + " and t." + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                    WhenMatchedAction = "UPDATE SET t."
                        + builder.TableBuilder.ValidToColumnNameEscaped + "=s." + builder.TableBuilder.ValidFromColumnNameEscaped
                        + (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + "=@EtlRunId" : "")
                        + (builder.TableBuilder.EtlRunToColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlRunToColumnNameEscaped + "=@EtlRunId" : ""),
                    Parameters = parameters2,
                };

                var noHistoryColumns = builder.TableBuilder.Table.Columns
                    .Where(x => x.GetHistoryDisabled() && !x.GetUsedByEtlRunInfo()).ToList();

                if (noHistoryColumns.Count > 0)
                {
                    var parameters3 = new Dictionary<string, object>();
                    if (builder.TableBuilder.EtlRunInsertColumnNameEscaped != null || builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null)
                        parameters3.Add("EtlRunId", currentEtlRunId);

                    yield return new CustomMsSqlMergeStatement(builder.TableBuilder.ResilientTable.Topic, "UpdateNoHistoryColumns")
                    {
                        ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                        CommandTimeout = 60 * 60,
                        SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                        TargetTableName = histTableName,
                        SourceTableAlias = "s",
                        TargetTableAlias = "t",
                        OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x)),
                        WhenMatchedAction = "UPDATE SET "
                            + string.Join(", ", noHistoryColumns.Select(col => "t." + col.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString) + " = s." + col.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)))
                            + (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + "=@EtlRunId" : ""),
                        Parameters = parameters3,
                    };
                }

                var copyToHistoryColumnNames = builder.TableBuilder.Table.Columns
                    .Where(x => !x.GetUsedByEtlRunInfo()
                        && !string.Equals(x.Name, builder.TableBuilder.ValidToColumnName, StringComparison.InvariantCulture))
                    .Select(c => c.NameEscaped(builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString))
                    .ToArray();

                var columnDefaults = new Dictionary<string, object>();

                if (builder.TableBuilder.ValidToColumnNameEscaped != null)
                    columnDefaults.Add(builder.TableBuilder.ValidToColumnNameEscaped, builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

                if (builder.TableBuilder.EtlRunInsertColumnNameEscaped != null)
                    columnDefaults.Add(builder.TableBuilder.EtlRunInsertColumnNameEscaped, currentEtlRunId);

                if (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null)
                    columnDefaults.Add(builder.TableBuilder.EtlRunUpdateColumnNameEscaped, currentEtlRunId);

                if (builder.TableBuilder.EtlRunFromColumnNameEscaped != null)
                    columnDefaults.Add(builder.TableBuilder.EtlRunFromColumnNameEscaped, currentEtlRunId);

                yield return new CopyTableIntoExistingTable(builder.TableBuilder.ResilientTable.Topic, "CopyToHistory")
                {
                    ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                    Configuration = new TableCopyConfiguration()
                    {
                        SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                        TargetTableName = histTableName,
                        ColumnConfiguration = copyToHistoryColumnNames
                            .Select(x => new ColumnCopyConfiguration(x))
                            .ToList()
                    },
                    ColumnDefaults = columnDefaults,
                    CommandTimeout = 60 * 60,
                };
            }
        }
    }
}