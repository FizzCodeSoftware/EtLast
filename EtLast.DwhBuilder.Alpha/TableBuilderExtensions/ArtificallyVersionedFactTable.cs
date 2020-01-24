namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static class ArtificallyVersionedFactTableExtension
    {
        public static DwhTableBuilder[] ArtificallyVersionedFactTable(this DwhTableBuilder[] builders, ConnectionStringWithProvider connectionString, string[] dimensionColumns, ColumnCopyConfiguration[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(builder => ExpandWithPreviousValue(builder, connectionString, dimensionColumns, valueColumns, customRowEqualityComparer));
                builder.AddFinalizerCreator(builder => CreateFactTableWithDateIntervalFinalizer(builder, dimensionColumns, valueColumns));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateFactTableWithDateIntervalFinalizer(DwhTableBuilder builder, string[] dimensionColumns, ColumnCopyConfiguration[] valueColumns)
        {
            var pk = builder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());
            var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();
            var currentEtlRunId = builder.DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            IEnumerable<SqlColumn> tempColumns = builder.SqlTable.Columns;
            if (useEtlRunTable)
            {
                tempColumns = tempColumns
                    .Where(x => x.Name != builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName && x.Name != builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName);
            }

            if (pkIsIdentity)
            {
                tempColumns = tempColumns
                    .Where(x => !pk.SqlColumns.Any(pkc => pkc.SqlColumn == x));
            }

            var columnsToInsert = tempColumns.ToList();

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "ClosePreviousValue")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.Table.TempTableName,
                SourceTableAlias = "s",
                TargetTableName = builder.Table.TableName,
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", dimensionColumns.Select(x => "t." + builder.DwhBuilder.ConnectionString.Escape(x) + " = s." + builder.DwhBuilder.ConnectionString.Escape(x)))
                    + " and t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName) + " = @InfiniteFuture",
                WhenMatchedAction = "UPDATE SET t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName) + "=s." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidFromColumnName)
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                Parameters = new Dictionary<string, object>
                {
                    ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                },
            };

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "MergeIntoBase")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.Table.TempTableName,
                SourceTableAlias = "s",
                TargetTableName = builder.Table.TableName,
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", dimensionColumns.Select(x => "t." + builder.DwhBuilder.ConnectionString.Escape(x) + " = s." + builder.DwhBuilder.ConnectionString.Escape(x)))
                    + " and " + string.Join(" and ", valueColumns.Select(x => "t." + builder.DwhBuilder.ConnectionString.Escape(x.FromColumn) + " = s." + builder.DwhBuilder.ConnectionString.Escape(x.FromColumn))),
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", columnsToInsert.Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name)))
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName + ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName : "")
                    + ") VALUES (" + string.Join(", ", columnsToInsert.Select(c => "s." + builder.DwhBuilder.ConnectionString.Escape(c.Name)))
                    + (useEtlRunTable ? ", " + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) + ", " + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : "")
                    + ")",
                Parameters = new Dictionary<string, object>
                {
                    ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                },
            };
        }

        private static IEnumerable<IRowOperation> ExpandWithPreviousValue(DwhTableBuilder builder, ConnectionStringWithProvider connectionString, string[] dimensionColumns, ColumnCopyConfiguration[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            var rowEqualityComparer = customRowEqualityComparer ??
                new ColumnBasedRowEqualityComparer()
                {
                    Columns = valueColumns.Select(x => x.FromColumn).ToArray(),
                };

            yield return new ExpandOperation()
            {
                InstanceName = "ExpandWithPreviousValue",
                RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.DwhBuilder.Context, "PreviousValueReader")
                {
                    ConnectionString = connectionString,
                    Sql = "select " + string.Join(",", dimensionColumns.Concat(valueColumns.Select(x => x.FromColumn)))
                        + " from " + connectionString.Escape(builder.SqlTable.SchemaAndTableName.TableName, builder.SqlTable.SchemaAndTableName.Schema)
                        + " where " + builder.DwhBuilder.Configuration.ValidToColumnName + "=@InfiniteFuture",
                    Parameters = new Dictionary<string, object>
                    {
                        ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                    },
                    ColumnConfiguration = dimensionColumns
                        .Select(x => new ReaderColumnConfiguration(x, new StringConverter())).ToList(),
                },
                LeftKeySelector = row => string.Join("\0", dimensionColumns.Select(c => row[c]?.ToString() ?? "-")),
                RightKeySelector = row => string.Join("\0", dimensionColumns.Select(c => row[c]?.ToString() ?? "-")),
                ColumnConfiguration = valueColumns.ToList(),
                NoMatchAction = new NoMatchAction(MatchMode.Custom)
                {
                    CustomAction = (op, row) =>
                    {
                        // this is the first version
                        row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, builder.DwhBuilder.Configuration.InfinitePastDateTime, op);
                        row.SetValue(builder.DwhBuilder.Configuration.ValidToColumnName, builder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                    }
                },
                MatchCustomAction = (op, row, match) =>
                {
                    if (rowEqualityComparer.Compare(row, match))
                    {
                        // there is at least one existing version, but this is the same as the last
                        op.Process.RemoveRow(row, op);
                    }
                    else
                    {
                        // this is different than the last version
                        row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, DateTime.Now, op);
                        row.SetValue(builder.DwhBuilder.Configuration.ValidToColumnName, builder.DwhBuilder.Configuration.InfiniteFutureDateTime, op);
                    }
                },
            };
        }
    }
}