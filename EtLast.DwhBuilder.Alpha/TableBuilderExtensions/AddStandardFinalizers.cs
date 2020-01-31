namespace FizzCode.EtLast.DwhBuilder.Alpha
{
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
            var pkColumnName = pk.SqlColumns[0].SqlColumn.Name;
            var currentEtlRunId = builder.DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();

            IEnumerable<SqlColumn> tempColumns = builder.SqlTable.Columns;
            if (useEtlRunTable)
            {
                tempColumns = tempColumns.Where(x => x.Name != builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName && x.Name != builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName);
            }

            var headColumnsToUpdate = tempColumns.Where(x => x.Name != builder.DwhBuilder.Configuration.ValidToColumnName).ToList();
            var headColumnsToInsert = tempColumns.Where(x => x.Name != builder.DwhBuilder.Configuration.ValidToColumnName).ToList();

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "MergeIntoBase")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.Table.TempTableName,
                SourceTableAlias = "s",
                TargetTableName = builder.Table.TableName,
                TargetTableAlias = "t",
                OnCondition = "t." + builder.DwhBuilder.ConnectionString.Escape(pkColumnName) + " = s." + builder.DwhBuilder.ConnectionString.Escape(pkColumnName),
                WhenMatchedAction = "UPDATE SET " + string.Join(", ", headColumnsToUpdate.Select(c => "t." + builder.DwhBuilder.ConnectionString.Escape(c.Name) + "=s." + builder.DwhBuilder.ConnectionString.Escape(c.Name)))
                                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", headColumnsToInsert.Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name)))
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName + ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName : "")
                    + ") VALUES (" + string.Join(", ", headColumnsToInsert.Select(c => "s." + builder.DwhBuilder.ConnectionString.Escape(c.Name)))
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
                    SourceTableAlias = "s",
                    TargetTableName = histTableName,
                    TargetTableAlias = "t",
                    OnCondition = "t." + builder.DwhBuilder.ConnectionString.Escape(pkColumnName) + " = s." + builder.DwhBuilder.ConnectionString.Escape(pkColumnName) + " and t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName) + " = @InfiniteFuture",
                    WhenMatchedAction = "UPDATE SET t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName) + "=s." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidFromColumnName)
                                        + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                    Parameters = new Dictionary<string, object>
                    {
                        ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                    },
                };

                var noHistoryColumns = tempColumns.Where(x => x.HasProperty<NoHistoryColumnProperty>()).ToList();
                if (noHistoryColumns.Count > 0)
                {
                    yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "UpdateNoHistory")
                    {
                        ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                        CommandTimeout = 60 * 60,
                        SourceTableName = builder.Table.TempTableName,
                        SourceTableAlias = "s",
                        TargetTableName = histTableName,
                        TargetTableAlias = "t",
                        OnCondition = "t." + builder.DwhBuilder.ConnectionString.Escape(pkColumnName) + " = s." + builder.DwhBuilder.ConnectionString.Escape(pkColumnName),
                        WhenMatchedAction = "UPDATE SET " + string.Join(", ", noHistoryColumns.Select(col => "t." + builder.DwhBuilder.ConnectionString.Escape(col.Name) + " = s." + builder.DwhBuilder.ConnectionString.Escape(col.Name)))
                                            + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                    };
                }

                var allColumnsExceptValidTo = tempColumns.Where(x => x.Name != builder.DwhBuilder.Configuration.ValidToColumnName).ToList();
                yield return new CopyTableIntoExistingTableProcess(builder.DwhBuilder.Context, "CopyToHist")
                {
                    ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                    Configuration = new TableCopyConfiguration()
                    {
                        SourceTableName = builder.Table.TempTableName,
                        TargetTableName = histTableName,
                        ColumnConfiguration = allColumnsExceptValidTo.Select(x => new ColumnCopyConfiguration(x.Name)).ToList(),
                    },
                    ColumnDefaults = builder.DwhBuilder.Configuration.UseEtlRunTable
                        ? new Dictionary<string, object>()
                        {
                            [builder.Table.Scope.Configuration.ConnectionString.Escape(builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName)] = currentEtlRunId,
                            [builder.Table.Scope.Configuration.ConnectionString.Escape(builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName)] = currentEtlRunId
                        }
                        : null,
                    CommandTimeout = 60 * 60,
                };
            }
        }
    }
}