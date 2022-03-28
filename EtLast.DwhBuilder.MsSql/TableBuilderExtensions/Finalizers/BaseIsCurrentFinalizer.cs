namespace FizzCode.EtLast.DwhBuilder.MsSql;

public static partial class TableBuilderExtensions
{
    public static DwhTableBuilder[] BaseIsCurrentFinalizer(this DwhTableBuilder[] builders, Action<KeyBasedFinalizerBuilder> customizer)
    {
        foreach (var tableBuilder in builders)
        {
            var tempBuilder = new KeyBasedFinalizerBuilder(tableBuilder);
            customizer.Invoke(tempBuilder);

            if (tempBuilder.MatchColumns == null)
                throw new NotSupportedException("you must specify the key columns of " + nameof(BaseIsCurrentFinalizer) + " for table " + tableBuilder.ResilientTable.TableName);

            tableBuilder.AddFinalizerCreator(_ => CreateBaseIsCurrentFinalizer(tempBuilder));
        }

        return builders;
    }

    private static IEnumerable<IExecutable> CreateBaseIsCurrentFinalizer(KeyBasedFinalizerBuilder builder)
    {
        var hasHistoryTable = builder.TableBuilder.Table.GetHasHistoryTable();

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

        var parameters1 = new Dictionary<string, object>();
        if (builder.TableBuilder.HasEtlRunInfo)
        {
            parameters1.Add("EtlRunId", builder.TableBuilder.DwhBuilder.EtlRunId.Value);
        }

        var columnNamesToUpdate = builder.TableBuilder.Table.Columns
            .Where(x => !x.GetUsedByEtlRunInfo()
                && !x.IsPrimaryKey
                && !builder.MatchColumns.Contains(x))
            .Select(c => c.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString))
            .ToArray();

        yield return new CustomMsSqlMergeStatement(builder.TableBuilder.ResilientTable.Scope.Context)
        {
            Name = "MergeIntoBase",
            ConnectionString = builder.TableBuilder.ResilientTable.Scope.ConnectionString,
            CommandTimeout = 60 * 60,
            SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
            TargetTableName = builder.TableBuilder.ResilientTable.TableName,
            SourceTableAlias = "s",
            TargetTableAlias = "t",
            OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
            WhenMatchedAction = columnNamesToUpdate.Length > 0 || builder.TableBuilder.HasEtlRunInfo
                ? "UPDATE SET "
                    + string.Join(", ", columnNamesToUpdate.Select(c => "t." + c + "=s." + c))
                    + (builder.TableBuilder.HasEtlRunInfo
                        ? (columnNamesToUpdate.Length > 0 ? ", " : "") + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + "=@EtlRunId"
                        : "")
                : null,
            WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", mergeIntoBaseColumnNames)
                + (builder.TableBuilder.HasEtlRunInfo
                    ? ", " + builder.TableBuilder.EtlRunInsertColumnNameEscaped + ", " + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + ", " + builder.TableBuilder.EtlRunFromColumnNameEscaped
                    : "")
                + ") VALUES ("
                    + string.Join(", ", mergeIntoBaseColumnNames.Select(c => "s." + c))
                    + (builder.TableBuilder.HasEtlRunInfo ? ", @EtlRunId, @EtlRunId, @EtlRunId" : "")
                + ")",
            Parameters = parameters1,
        };

        if (hasHistoryTable)
        {
            var histTableName = builder.TableBuilder.DwhBuilder.GetEscapedHistTableName(builder.TableBuilder.Table);

            var parameters2 = new Dictionary<string, object>();
            if (builder.TableBuilder.HasEtlRunInfo)
                parameters2["EtlRunId"] = builder.TableBuilder.DwhBuilder.EtlRunId.Value;

            if (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime != null)
                parameters2["InfiniteFuture"] = builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime;

            yield return new CustomMsSqlMergeStatement(builder.TableBuilder.ResilientTable.Scope.Context)
            {
                Name = "CloseOpenEndedHistoryRecords",
                ConnectionString = builder.TableBuilder.ResilientTable.Scope.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                TargetTableName = histTableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))"))
                    + " and t." + builder.TableBuilder.ValidToColumnNameEscaped + (builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime == null ? " IS NULL" : "=@InfiniteFuture"),
                WhenMatchedAction = "UPDATE SET t."
                    + builder.TableBuilder.ValidToColumnNameEscaped + "=s." + builder.TableBuilder.ValidFromColumnNameEscaped
                    + (builder.TableBuilder.HasEtlRunInfo
                        ? ", " + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + "=@EtlRunId, " + builder.TableBuilder.EtlRunToColumnNameEscaped + "=@EtlRunId"
                        : ""),
                Parameters = parameters2,
            };

            var noHistoryColumns = builder.TableBuilder.Table.Columns
                .Where(x => x.GetHistoryDisabled() && !x.GetUsedByEtlRunInfo()).ToList();

            if (noHistoryColumns.Count > 0)
            {
                var parameters3 = new Dictionary<string, object>();
                if (builder.TableBuilder.HasEtlRunInfo)
                    parameters3["EtlRunId"] = builder.TableBuilder.DwhBuilder.EtlRunId.Value;

                yield return new CustomMsSqlMergeStatement(builder.TableBuilder.ResilientTable.Scope.Context)
                {
                    Name = "UpdateNoHistoryColumns",
                    ConnectionString = builder.TableBuilder.ResilientTable.Scope.ConnectionString,
                    CommandTimeout = 60 * 60,
                    SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                    TargetTableName = histTableName,
                    SourceTableAlias = "s",
                    TargetTableAlias = "t",
                    OnCondition = string.Join(" and ", columnNamesToMatch.Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
                    WhenMatchedAction = "UPDATE SET "
                        + string.Join(", ", noHistoryColumns.Select(col => "t." + col.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString) + " = s." + col.NameEscaped(builder.TableBuilder.DwhBuilder.ConnectionString)))
                        + (builder.TableBuilder.HasEtlRunInfo
                            ? ", " + builder.TableBuilder.EtlRunUpdateColumnNameEscaped + "=@EtlRunId"
                            : ""),
                    Parameters = parameters3,
                };
            }

            var copyToHistoryColumnNames = builder.TableBuilder.Table.Columns
                .Where(x => !x.GetUsedByEtlRunInfo()
                    && !string.Equals(x.Name, builder.TableBuilder.ValidToColumnName, StringComparison.InvariantCulture))
                .Select(c => c.NameEscaped(builder.TableBuilder.ResilientTable.Scope.ConnectionString))
                .ToArray();

            var columnDefaults = new Dictionary<string, object>();

            if (builder.TableBuilder.ValidToColumnNameEscaped != null)
                columnDefaults[builder.TableBuilder.ValidToColumnNameEscaped] = builder.TableBuilder.DwhBuilder.Configuration.InfiniteFutureDateTime;

            if (builder.TableBuilder.HasEtlRunInfo)
            {
                columnDefaults[builder.TableBuilder.EtlRunInsertColumnNameEscaped] = builder.TableBuilder.DwhBuilder.EtlRunId.Value;
                columnDefaults[builder.TableBuilder.EtlRunUpdateColumnNameEscaped] = builder.TableBuilder.DwhBuilder.EtlRunId.Value;
                columnDefaults[builder.TableBuilder.EtlRunFromColumnNameEscaped] = builder.TableBuilder.DwhBuilder.EtlRunId.Value;
            }

            yield return new CopyTableIntoExistingTable(builder.TableBuilder.ResilientTable.Scope.Context)
            {
                Name = "CopyToHistory",
                ConnectionString = builder.TableBuilder.ResilientTable.Scope.ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = builder.TableBuilder.ResilientTable.TempTableName,
                    TargetTableName = histTableName,
                    Columns = copyToHistoryColumnNames.ToDictionary(x => x),
                },
                ColumnDefaults = columnDefaults,
                CommandTimeout = 60 * 60,
            };
        }
    }
}
