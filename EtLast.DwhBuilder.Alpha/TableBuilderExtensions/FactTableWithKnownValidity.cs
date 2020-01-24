namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static class FactTableWithKnownValidityExtension
    {
        public static DwhTableBuilder[] FactTableWithKnownValidity(this DwhTableBuilder[] builders, string[] dimensionColumns, string[] valueColumns)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(builder => CreateOperations(builder, dimensionColumns, valueColumns));
                builder.AddFinalizerCreator(builder => CreateFinalizers(builder, dimensionColumns, valueColumns));
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateFinalizers(DwhTableBuilder builder, string[] dimensionColumns, string[] valueColumns)
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

            yield return new CustomMsSqlMergeSqlStatementProcess(builder.DwhBuilder.Context, "MergeIntoBase")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.Table.TempTableName,
                SourceTableAlias = "s",
                TargetTableName = builder.Table.TableName,
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", dimensionColumns.Select(x => "t." + builder.DwhBuilder.ConnectionString.Escape(x) + " = s." + builder.DwhBuilder.ConnectionString.Escape(x)))
                    + " and t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidFromColumnName) + " = s." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidFromColumnName)
                    + " and t." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName) + " = s." + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName),
                WhenMatchedAction = "UPDATE SET " + string.Join(",", valueColumns.Select(c => "t." + c + "=s." + c))
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName + "=" + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : ""),
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", columnsToInsert.Select(c => builder.DwhBuilder.ConnectionString.Escape(c.Name)))
                    + (useEtlRunTable ? ", " + builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName + ", " + builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName : "")
                    + ") VALUES (" + string.Join(", ", columnsToInsert.Select(c => "s." + builder.DwhBuilder.ConnectionString.Escape(c.Name)))
                    + (useEtlRunTable ? ", " + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) + ", " + currentEtlRunId.ToString("D", CultureInfo.InvariantCulture) : "")
                    + ")",
                // todo: update values on match
                Parameters = new Dictionary<string, object>
                {
                    ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                },
            };
        }

        private static IEnumerable<IRowOperation> CreateOperations(DwhTableBuilder builder, string[] dimensionColumns, string[] valueColumns, IRowEqualityComparer customRowEqualityComparer = null)
        {
            var rowEqualityComparer = customRowEqualityComparer ??
                new ColumnBasedRowEqualityComparer()
                {
                    Columns = valueColumns
                        .Concat(new[] { builder.DwhBuilder.Configuration.ValidFromColumnName, builder.DwhBuilder.Configuration.ValidToColumnName })
                        .ToArray(),
                };

            yield return new ExpandOperation()
            {
                InstanceName = "ExpandWithPreviousValue",
                RightProcess = new CustomSqlAdoNetDbReaderProcess(builder.DwhBuilder.Context, "PreviousValueReader")
                {
                    ConnectionString = builder.DwhBuilder.ConnectionString,
                    Sql = "select " + string.Join(",", dimensionColumns.Concat(valueColumns))
                        + ", " + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidFromColumnName)
                        + ", " + builder.DwhBuilder.ConnectionString.Escape(builder.DwhBuilder.Configuration.ValidToColumnName)
                        + " from " + builder.DwhBuilder.ConnectionString.Escape(builder.SqlTable.SchemaAndTableName.TableName, builder.SqlTable.SchemaAndTableName.Schema),
                    Parameters = new Dictionary<string, object>
                    {
                        ["InfiniteFuture"] = builder.DwhBuilder.Configuration.InfiniteFutureDateTime,
                    },
                    ColumnConfiguration = dimensionColumns
                        .Select(x => new ReaderColumnConfiguration(x, new StringConverter())).ToList(),
                },
                LeftKeySelector = row => string.Join("\0", dimensionColumns.Select(c => row[c]?.ToString() ?? "-")) + "/" + row[builder.DwhBuilder.Configuration.ValidFromColumnName].ToString() + "-" + row[builder.DwhBuilder.Configuration.ValidToColumnName].ToString(),
                RightKeySelector = row => string.Join("\0", dimensionColumns.Select(c => row[c]?.ToString() ?? "-")) + "/" + row[builder.DwhBuilder.Configuration.ValidFromColumnName].ToString() + "-" + row[builder.DwhBuilder.Configuration.ValidToColumnName].ToString(),
                ColumnConfiguration = new List<ColumnCopyConfiguration>(),
                MatchCustomAction = (op, row, match) =>
                {
                    if (rowEqualityComparer.Compare(row, match))
                    {
                        // row already exists with same values (and same dimensions)
                        op.Process.RemoveRow(row, op);
                    }
                },
            };
        }
    }
}