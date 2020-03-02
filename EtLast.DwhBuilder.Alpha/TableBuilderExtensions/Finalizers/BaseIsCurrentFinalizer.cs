namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] BaseIsCurrentFinalizer(this DwhTableBuilder[] builders, Action<KeyBasedFinalizerBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new KeyBasedFinalizerBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.KeyColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(BaseIsCurrentFinalizer) + " for table " + tableBuilder.Table.TableName);

                tableBuilder.AddFinalizerCreator(_ => CreateBaseIsCurrentFinalizer(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateBaseIsCurrentFinalizer(KeyBasedFinalizerBuilder builder)
        {
            var hasHistoryTable = builder.TableBuilder.SqlTable.HasProperty<WithHistoryTableProperty>();
            var pk = builder.TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());
            var currentEtlRunId = builder.TableBuilder.Table.Topic.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            var mergeIntoBaseColumns = builder.TableBuilder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>());

            if (pkIsIdentity)
            {
                mergeIntoBaseColumns = mergeIntoBaseColumns
                    .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)));
            }

            var mergeIntoBaseColumnNames = mergeIntoBaseColumns
                .Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c.Name))
                .ToArray();

            var columnNamesToMatch = builder.KeyColumns
                .Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c))
                .ToArray();

            var parameters1 = new Dictionary<string, object>();
            if (builder.TableBuilder.EtlInsertRunIdColumnNameEscaped != null || builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null)
                parameters1.Add("EtlRunId", currentEtlRunId);

            var columnNamesToUpdate = builder.TableBuilder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase))
                    && builder.KeyColumns.All(kc => !string.Equals(x.Name, kc, StringComparison.InvariantCultureIgnoreCase)))
                .Select(c => builder.TableBuilder.DwhBuilder.ConnectionString.Escape(c.Name))
                .ToArray();

            yield return new CustomMsSqlMergeStatement(builder.TableBuilder.Table.Topic, "MergeIntoBase")
            {
                ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.TableBuilder.Table.TempTableName,
                TargetTableName = builder.TableBuilder.Table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x)),
                WhenMatchedAction = columnNamesToUpdate.Length > 0 || (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null)
                    ? "UPDATE SET "
                        + string.Join(", ", columnNamesToUpdate.Select(c => "t." + c + "=s." + c))
                        + (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null
                            ? (columnNamesToUpdate.Length > 0 ? ", " : "") + builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped + "=@EtlRunId"
                            : "")
                    : null,
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", mergeIntoBaseColumnNames)
                    + (builder.TableBuilder.EtlInsertRunIdColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlInsertRunIdColumnNameEscaped : "")
                    + (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped : "")
                    + ") VALUES ("
                        + string.Join(", ", mergeIntoBaseColumnNames.Select(c => "s." + c))
                        + (builder.TableBuilder.EtlInsertRunIdColumnNameEscaped != null ? ", @EtlRunId" : "")
                        + (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null ? ", @EtlRunId" : "")
                    + ")",
                Parameters = parameters1,
            };

            if (hasHistoryTable)
            {
                var histTableName = builder.TableBuilder.DwhBuilder.GetEscapedHistTableName(builder.TableBuilder.SqlTable);

                var parameters2 = new Dictionary<string, object>();
                if (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null)
                    parameters2.Add("EtlRunId", currentEtlRunId);

                if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                    parameters2.Add("InfiniteFuture", builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

                yield return new CustomMsSqlMergeStatement(builder.TableBuilder.Table.Topic, "CloseOpenEndedHistoryRecords")
                {
                    ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                    CommandTimeout = 60 * 60,
                    SourceTableName = builder.TableBuilder.Table.TempTableName,
                    TargetTableName = histTableName,
                    SourceTableAlias = "s",
                    TargetTableAlias = "t",
                    OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x))
                        + " and t." + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                    WhenMatchedAction = "UPDATE SET t."
                        + builder.TableBuilder.ValidToColumnNameEscaped + "=s." + builder.TableBuilder.ValidFromColumnNameEscaped
                        + (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped + "=@EtlRunId" : ""),
                    Parameters = parameters2,
                };

                var noHistoryColumns = builder.TableBuilder.SqlTable.Columns
                    .Where(x => x.HasProperty<NoHistoryColumnProperty>() && !x.HasProperty<IsEtlRunInfoColumnProperty>()).ToList();

                if (noHistoryColumns.Count > 0)
                {
                    var parameters3 = new Dictionary<string, object>();
                    if (builder.TableBuilder.EtlInsertRunIdColumnNameEscaped != null || builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null)
                        parameters3.Add("EtlRunId", currentEtlRunId);

                    yield return new CustomMsSqlMergeStatement(builder.TableBuilder.Table.Topic, "UpdateNoHistoryColumns")
                    {
                        ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                        CommandTimeout = 60 * 60,
                        SourceTableName = builder.TableBuilder.Table.TempTableName,
                        TargetTableName = histTableName,
                        SourceTableAlias = "s",
                        TargetTableAlias = "t",
                        OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x)),
                        WhenMatchedAction = "UPDATE SET "
                            + string.Join(", ", noHistoryColumns.Select(col => "t." + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(col.Name) + " = s." + builder.TableBuilder.DwhBuilder.ConnectionString.Escape(col.Name)))
                            + (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped + "=@EtlRunId" : ""),
                        Parameters = parameters3,
                    };
                }

                var copyToHistoryColumnNames = builder.TableBuilder.SqlTable.Columns
                    .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                        && !string.Equals(x.Name, builder.TableBuilder.ValidToColumnName, StringComparison.InvariantCulture))
                    .Select(c => builder.TableBuilder.Table.Scope.Configuration.ConnectionString.Escape(c.Name))
                    .ToArray();

                var columnDefaults = new Dictionary<string, object>()
                {
                    [builder.TableBuilder.ValidToColumnNameEscaped] = builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                };

                if (builder.TableBuilder.EtlInsertRunIdColumnNameEscaped != null)
                    columnDefaults.Add(builder.TableBuilder.EtlInsertRunIdColumnNameEscaped, currentEtlRunId);

                if (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null)
                    columnDefaults.Add(builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped, currentEtlRunId);

                yield return new CopyTableIntoExistingTable(builder.TableBuilder.Table.Topic, "CopyToHistory")
                {
                    ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                    Configuration = new TableCopyConfiguration()
                    {
                        SourceTableName = builder.TableBuilder.Table.TempTableName,
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