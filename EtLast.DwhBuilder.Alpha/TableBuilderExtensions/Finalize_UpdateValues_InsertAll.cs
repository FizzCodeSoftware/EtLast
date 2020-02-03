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
        public static DwhTableBuilder[] Finalize_UpdateValues_InsertAll(this DwhTableBuilder[] builders, string[] keyColumns, string[] valueColumns)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(builder => Create_Finalize_UpdateValues_InsertAll(builder, keyColumns, valueColumns));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> Create_Finalize_UpdateValues_InsertAll(DwhTableBuilder builder, string[] keyColumns, string[] valueColumns)
        {
            var pk = builder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());
            var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();
            var currentEtlRunId = builder.DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0).ToString("D", CultureInfo.InvariantCulture);

            var columnsToInsert = builder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>());

            if (pkIsIdentity)
            {
                columnsToInsert = columnsToInsert
                    .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)));
            }

            var columnsNamesToInsert = columnsToInsert
                .Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name))
                .ToList();

            var columnsToMatch = keyColumns
                .Select(x => builder.DwhBuilder.ConnectionString.Escape(x))
                .ToList();

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "MergeIntoBase")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.Table.TempTableName,
                TargetTableName = builder.Table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnsToMatch.Select(x => "t." + x + " = s." + x)),
                WhenMatchedAction =
                    valueColumns.Length > 0 || useEtlRunTable
                        ? "UPDATE SET "
                            + string.Join(",", valueColumns.Select(c => "t." + c + "=s." + c))
                            + (useEtlRunTable ? (valueColumns.Length > 0 ? ", " : "") + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId : "")
                        : null,
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", columnsNamesToInsert)
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName + ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName : "")
                    + ") VALUES ("
                        + string.Join(", ", columnsNamesToInsert)
                        + (useEtlRunTable ? ", " + currentEtlRunId + ", " + currentEtlRunId : "")
                    + ")",
            };
        }
    }
}