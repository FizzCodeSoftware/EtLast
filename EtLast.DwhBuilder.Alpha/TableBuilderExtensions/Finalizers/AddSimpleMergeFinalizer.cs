namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] AddSimpleMergeFinalizer(this DwhTableBuilder[] builders, Action<KeyBasedFinalizerBuilder> customizer)
        {
            foreach (var tableBuilder in builders)
            {
                var tempBuilder = new KeyBasedFinalizerBuilder(tableBuilder);
                customizer.Invoke(tempBuilder);

                if (tempBuilder.KeyColumns == null)
                    throw new NotSupportedException("you must specify the key columns of " + nameof(RemoveExistingRows) + " for table " + tableBuilder.Table.TableName);

                tableBuilder.AddFinalizerCreator(_ => CreateAddSimpleMergeFinalizer(tempBuilder));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateAddSimpleMergeFinalizer(KeyBasedFinalizerBuilder builder)
        {
            var pk = builder.TableBuilder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());
            var currentEtlRunId = builder.TableBuilder.DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

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

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.TableBuilder.DwhBuilder.Context, "MergeIntoBase", builder.TableBuilder.Table.Topic)
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
        }
    }
}