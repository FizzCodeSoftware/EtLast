namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] BaseIsHistoryFinalizer(this DwhTableBuilder[] builders, Action<KeyBasedFinalizerBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new KeyBasedFinalizerBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.KeyColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(BaseIsHistoryFinalizer) + " for table " + tableBuilder.Table.TableName);

                tableBuilder.AddFinalizerCreator(_ => CreateBaseIsHistoryFinalizer(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateBaseIsHistoryFinalizer(KeyBasedFinalizerBuilder builder)
        {
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

            var parameters = new Dictionary<string, object>();
            if (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null)
                parameters.Add("EtlRunId", currentEtlRunId);

            if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                parameters.Add("InfiniteFuture", builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime);

            // todo: support NoHistoryColumnProperty

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.TableBuilder.Table.Topic, "CloseOpenEndedBaseRecords")
            {
                ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.TableBuilder.Table.TempTableName,
                TargetTableName = builder.TableBuilder.Table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "t." + x + "=s." + x))
                    + " and t." + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                WhenMatchedAction = "UPDATE SET t."
                    + builder.TableBuilder.ValidToColumnNameEscaped + "=s." + builder.TableBuilder.ValidFromColumnNameEscaped
                    + (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null ? ", " + builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped + "=@EtlRunId" : ""),
                Parameters = parameters,
            };

            var columnDefaults = new Dictionary<string, object>();

            if (builder.TableBuilder.EtlInsertRunIdColumnNameEscaped != null)
                columnDefaults.Add(builder.TableBuilder.EtlInsertRunIdColumnNameEscaped, currentEtlRunId);

            if (builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped != null)
                columnDefaults.Add(builder.TableBuilder.EtlUpdateRunIdColumnNameEscaped, currentEtlRunId);

            yield return new CopyTableIntoExistingTableProcess(builder.TableBuilder.Table.Topic, "CopyToBase")
            {
                ConnectionString = builder.TableBuilder.Table.Scope.Configuration.ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = builder.TableBuilder.Table.TempTableName,
                    TargetTableName = builder.TableBuilder.Table.TableName,
                    ColumnConfiguration = mergeIntoBaseColumnNames.Select(x => new ColumnCopyConfiguration(x)).ToList(),
                },
                ColumnDefaults = columnDefaults,
                CommandTimeout = 60 * 60,
            };
        }
    }
}