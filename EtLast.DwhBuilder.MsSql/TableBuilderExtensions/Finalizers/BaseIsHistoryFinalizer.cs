namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] BaseIsHistoryFinalizer(this DwhTableBuilder[] builders, Action<KeyBasedFinalizerBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new KeyBasedFinalizerBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.MatchColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(BaseIsHistoryFinalizer) + " for table " + tableBuilder.ResilientTable.TableName);

                tableBuilder.AddFinalizerCreator(_ => CreateBaseIsHistoryFinalizer(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateBaseIsHistoryFinalizer(KeyBasedFinalizerBuilder builder)
        {
            var currentEtlRunId = builder.TableBuilder.ResilientTable.Topic.Context.AdditionalData.GetAs("CurrentEtlRunId", DateTimeOffset.Now);

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

            var parameters = new Dictionary<string, object>();
            if (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null)
                parameters.Add("EtlRunId", currentEtlRunId);

            if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                parameters.Add("InfiniteFuture", builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

            // todo: support NoHistoryColumnProperty

            yield return new CustomMsSqlMergeStatement(builder.TableBuilder.ResilientTable.Topic, "CloseOpenEndedBaseRecords")
            {
                ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                TargetTableName = builder.TableBuilder.ResilientTable.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x))
                    + " and t." + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                WhenMatchedAction = "UPDATE SET t."
                    + builder.TableBuilder.ValidToColumnNameEscaped + "=s." + builder.TableBuilder.ValidFromColumnNameEscaped
                    + (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + "=@EtlRunId" : ""),
                Parameters = parameters,
            };

            var columnDefaults = new Dictionary<string, object>();

            if (builder.TableBuilder.EtlRunInsertColumnNameEscaped != null)
                columnDefaults.Add(builder.TableBuilder.EtlRunInsertColumnNameEscaped, currentEtlRunId);

            if (builder.TableBuilder.EtlRunUpdateColumnNameEscaped != null)
                columnDefaults.Add(builder.TableBuilder.EtlRunUpdateColumnNameEscaped, currentEtlRunId);

            yield return new CopyTableIntoExistingTable(builder.TableBuilder.ResilientTable.Topic, "CopyToBase")
            {
                ConnectionString = builder.TableBuilder.ResilientTable.Scope.Configuration.ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                    TargetTableName = builder.TableBuilder.ResilientTable.TableName,
                    ColumnConfiguration = mergeIntoBaseColumnNames.Select(x => new ColumnCopyConfiguration(x)).ToList(),
                },
                ColumnDefaults = columnDefaults,
                CommandTimeout = 60 * 60,
            };
        }
    }
}