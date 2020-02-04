namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        /// <summary>
        /// - merges all columns from the temp to the target table with a default merger based on the already-filled PK column
        /// - if temp table is enabled then merges all columns from the temp to the history table based on the PK column (and ValidFromColumnName and ValidToColumnName)
        /// - maintains EtlInsertRunIdColumnName/EtlUpdateRunIdColumnName values if enabled
        /// </summary>
        public static DwhTableBuilder[] AddStandardFinalizers(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(FinalizerCreator);
            }

            return builders;
        }

        private static IEnumerable<IExecutable> FinalizerCreator(DwhTableBuilder builder)
        {
            var hasHistory = !builder.SqlTable.HasProperty<NoHistoryTableProperty>();
            var pk = builder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());
            var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();
            var currentEtlRunId = builder.DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            var columnsToInsert = builder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>());

            var columnsToUpdate = builder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>());

            if (pkIsIdentity)
            {
                columnsToInsert = columnsToInsert
                    .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)));
            }

            columnsToUpdate = columnsToUpdate
                .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)));

            var columnNamesToInsert = columnsToInsert
                .Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name))
                .ToArray();

            var noHistoryColumns = builder.SqlTable.Columns
                .Where(x => x.HasProperty<NoHistoryColumnProperty>() && !x.HasProperty<IsEtlRunInfoColumnProperty>()).ToList();

            var columnNamesToUpdate = columnsToUpdate
                .Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name))
                .ToArray();

            var columnNamesToMatch = pk.SqlColumns
                .Select(c => builder.DwhBuilder.ConnectionString.Escape(c.SqlColumn.Name))
                .ToArray();

            var columnNamesToInsertHistory = builder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>()
                    && !string.Equals(x.Name, builder.DwhBuilder.Configuration.ValidToColumnName, StringComparison.InvariantCulture))
                .Select(x => new ColumnCopyConfiguration(builder.Table.Scope.Configuration.ConnectionString.Escape(x.Name)))
                .ToList();

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "MergeIntoBase")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.Table.TempTableName,
                TargetTableName = builder.Table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + " = s." + x)),
                WhenMatchedAction = "UPDATE SET " + string.Join(", ", columnNamesToUpdate.Select(c => "t." + c + "=s." + c))
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", columnNamesToInsert)
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName + ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName : "")
                    + ") VALUES (" + string.Join(", ", columnNamesToInsert.Select(c => "s." + c))
                    + (useEtlRunTable ? ", " + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) + ", " + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : "")
                    + ")",
            };

            if (hasHistory)
            {
                var histTableName = builder.DwhBuilder.GetEscapedHistTableName(builder.SqlTable);

                yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "CloseLastHistIntervals")
                {
                    ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                    CommandTimeout = 60 * 60,
                    SourceTableName = builder.Table.TempTableName,
                    TargetTableName = histTableName,
                    SourceTableAlias = "s",
                    TargetTableAlias = "t",
                    OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + " = s." + x))
                        + " and t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName)
                        + (builder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : " = @InfiniteFuture"),
                    WhenMatchedAction = "UPDATE SET t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName) + "=s." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidFromColumnName)
                        + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                    Parameters = builder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? null : new Dictionary<string, object>
                    {
                        ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                    },
                };

                if (noHistoryColumns.Count > 0)
                {
                    yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "UpdateNoHistory")
                    {
                        ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                        CommandTimeout = 60 * 60,
                        SourceTableName = builder.Table.TempTableName,
                        TargetTableName = histTableName,
                        SourceTableAlias = "s",
                        TargetTableAlias = "t",
                        OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + " = s." + x)),
                        WhenMatchedAction = "UPDATE SET " + string.Join(", ", noHistoryColumns.Select(col => "t." + builder.DwhBuilder.ConnectionString.Escape(col.Name) + " = s." + builder.DwhBuilder.ConnectionString.Escape(col.Name)))
                            + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                    };
                }

                var columnDefaults = new Dictionary<string, object>()
                {
                    [builder.Table.Scope.Configuration.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName)] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                };

                if (builder.DwhBuilder.Configuration.UseEtlRunTable)
                {
                    columnDefaults.Add(builder.Table.Scope.Configuration.ConnectionString.Escape(builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName), currentEtlRunId);
                    columnDefaults.Add(builder.Table.Scope.Configuration.ConnectionString.Escape(builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName), currentEtlRunId);
                }

                yield return new CopyTableIntoExistingTableProcess(builder.DwhBuilder.Context, "CopyToHist")
                {
                    ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                    Configuration = new TableCopyConfiguration()
                    {
                        SourceTableName = builder.Table.TempTableName,
                        TargetTableName = histTableName,
                        ColumnConfiguration = columnNamesToInsertHistory.ToList(),
                    },
                    ColumnDefaults = columnDefaults,
                    CommandTimeout = 60 * 60,
                };
            }
        }
    }
}