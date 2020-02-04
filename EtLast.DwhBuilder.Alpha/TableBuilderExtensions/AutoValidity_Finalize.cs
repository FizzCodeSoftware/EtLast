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
        public static DwhTableBuilder[] AutoValidity_Finalize(this DwhTableBuilder[] builders, string[] keyColumns, ColumnCopyConfiguration[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(builder => CreateAutoValidity_Finalize(builder, keyColumns, valueColumns));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateAutoValidity_Finalize(DwhTableBuilder builder, string[] keyColumns, ColumnCopyConfiguration[] valueColumns)
        {
            var pk = builder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());
            var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();
            var currentEtlRunId = builder.DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            var columnsToInsert = builder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>());

            if (pkIsIdentity)
            {
                columnsToInsert = columnsToInsert
                    .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)));
            }

            var columnNamesToInsert = columnsToInsert
                .Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name))
                .ToList();

            var columnsToMatch = keyColumns
                .Select(x => builder.DwhBuilder.ConnectionString.Escape(x))
                .ToList();

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "CloseOpenEndedValues")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.Table.TempTableName,
                SourceTableAlias = "s",
                TargetTableName = builder.Table.TableName,
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnsToMatch.Select(x => "t." + x + " = s." + x))
                    + " and t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName)
                    + (builder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : " = @InfiniteFuture"),
                WhenMatchedAction = "UPDATE SET t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName) + "=s." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidFromColumnName)
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                Parameters = builder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? null : new Dictionary<string, object>
                {
                    ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                },
            };

            yield return new CopyTableIntoExistingTableProcess(builder.DwhBuilder.Context, "CopyNewValues")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = builder.Table.TempTableName,
                    TargetTableName = builder.Table.TableName,
                    ColumnConfiguration = columnNamesToInsert.Select(x => new ColumnCopyConfiguration(x)).ToList(),
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