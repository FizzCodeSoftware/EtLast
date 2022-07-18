namespace FizzCode.EtLast;

public static class ResilientSqlFinalizerExtensions
{
    public static ResilientSqlTableTableFinalizerBuilder TruncateTargetTable(this ResilientSqlTableTableFinalizerBuilder builder, int commandTimeout = 60 * 60)
    {
        return builder.Add(new TruncateTable(builder.Table.Scope.Context)
        {
            Name = "TruncateTargetTableFinalizer",
            ConnectionString = builder.Table.Scope.ConnectionString,
            TableName = builder.Table.TableName,
            CommandTimeout = commandTimeout,
        });
    }

    public static ResilientSqlTableTableFinalizerBuilder DeleteTargetTable(this ResilientSqlTableTableFinalizerBuilder builder, string customWhereClause = null, int commandTimeout = 60 * 60)
    {
        return builder.Add(new DeleteTable(builder.Table.Scope.Context)
        {
            Name = "DeleteTargetTableFinalizer",
            ConnectionString = builder.Table.Scope.ConnectionString,
            TableName = builder.Table.TableName,
            CommandTimeout = commandTimeout,
            CustomWhereClause = customWhereClause,
        });
    }

    public static ResilientSqlTableTableFinalizerBuilder CopyTable(this ResilientSqlTableTableFinalizerBuilder builder, int commandTimeout = 60 * 60, bool copyIdentityColumns = false)
    {
        if (copyIdentityColumns && builder.Table.Columns == null)
            throw new EtlException(builder.Table.Scope, "identity columns can be copied only if the " + nameof(ResilientTable) + "." + nameof(ResilientTableBase.Columns) + " is specified");

        return builder.Add(new CopyTableIntoExistingTable(builder.Table.Scope.Context)
        {
            Name = "CopyTableFinalizer",
            ConnectionString = builder.Table.Scope.ConnectionString,
            Configuration = new TableCopyConfiguration()
            {
                SourceTableName = builder.Table.TempTableName,
                TargetTableName = builder.Table.TableName,
                Columns = builder.Table.Columns?.ToDictionary(x => x, x => builder.Table.Scope.ConnectionString.Escape(x)),
            },
            CommandTimeout = commandTimeout,
            CopyIdentityColumns = copyIdentityColumns,
        });
    }

    public static ResilientSqlTableTableFinalizerBuilder SimpleMsSqlMerge(this ResilientSqlTableTableFinalizerBuilder builder, string keyColumn, int commandTimeout = 60 * 60)
    {
        var columnsToUpdate = builder.Table.Columns
            .Where(x => !string.Equals(x, keyColumn, StringComparison.InvariantCultureIgnoreCase))
            .ToList();

        keyColumn = builder.Table.Scope.ConnectionString.Escape(keyColumn);

        return builder.Add(new CustomMsSqlMergeStatement(builder.Table.Scope.Context)
        {
            Name = "SimpleMergeFinalizer",
            ConnectionString = builder.Table.Scope.ConnectionString,
            CommandTimeout = commandTimeout,
            SourceTableName = builder.Table.TempTableName,
            TargetTableName = builder.Table.TableName,
            SourceTableAlias = "s",
            TargetTableAlias = "t",
            OnCondition = "((s." + keyColumn + "=t." + keyColumn + ") or (s." + keyColumn + " is null and t." + keyColumn + " is null))",
            WhenMatchedAction = columnsToUpdate.Count > 0
                ? "update set " + string.Join(",", columnsToUpdate
                    .Select(x => builder.Table.Scope.ConnectionString.Escape(x))
                    .Select(x => "t." + x + "=s." + x))
                : null,
            WhenNotMatchedByTargetAction = "insert ("
                + string.Join(",", builder.Table.Columns
                    .Select(x => builder.Table.Scope.ConnectionString.Escape(x))) + ") values ("
                + string.Join(",", builder.Table.Columns
                    .Select(x => "s." + builder.Table.Scope.ConnectionString.Escape(x))) + ")",
        });
    }

    public static ResilientSqlTableTableFinalizerBuilder SimpleMsSqlMerge(this ResilientSqlTableTableFinalizerBuilder builder, string[] keyColumns, int commandTimeout = 60 * 60)
    {
        var columnsToUpdate = builder.Table.Columns
            .Where(x => !keyColumns.Any(keyColumn => string.Equals(x, keyColumn, StringComparison.InvariantCultureIgnoreCase)))
            .ToList();

        return builder.Add(new CustomMsSqlMergeStatement(builder.Table.Scope.Context)
        {
            Name = "SimpleMergeFinalizer",
            ConnectionString = builder.Table.Scope.ConnectionString,
            CommandTimeout = commandTimeout,
            SourceTableName = builder.Table.TempTableName,
            TargetTableName = builder.Table.TableName,
            SourceTableAlias = "s",
            TargetTableAlias = "t",
            OnCondition = string.Join(" and ", keyColumns
                .Select(x => builder.Table.Scope.ConnectionString.Escape(x))
                .Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
            WhenMatchedAction = columnsToUpdate.Count > 0
                ? "update set " + string.Join(",", columnsToUpdate
                    .Select(x => builder.Table.Scope.ConnectionString.Escape(x))
                    .Select(x => "t." + x + "=s." + x))
                : null,
            WhenNotMatchedByTargetAction = "insert ("
                + string.Join(",", builder.Table.Columns
                    .Select(x => builder.Table.Scope.ConnectionString.Escape(x))) + ") values ("
                + string.Join(",", builder.Table.Columns
                    .Select(x => "s." + builder.Table.Scope.ConnectionString.Escape(x))) + ")",
        });
    }

    public static ResilientSqlTableTableFinalizerBuilder SimpleMsSqlMergeUpdateOnly(this ResilientSqlTableTableFinalizerBuilder builder, string[] keyColumns, int commandTimeout = 60 * 60)
    {
        var columnsToUpdate = builder.Table.Columns
            .Where(x => !keyColumns.Contains(x))
            .ToList();

        return builder.Add(new CustomMsSqlMergeStatement(builder.Table.Scope.Context)
        {
            Name = "SimpleMergeUpdateOnlyFinalizer",
            ConnectionString = builder.Table.Scope.ConnectionString,
            CommandTimeout = commandTimeout,
            SourceTableName = builder.Table.TempTableName,
            TargetTableName = builder.Table.TableName,
            SourceTableAlias = "s",
            TargetTableAlias = "t",
            OnCondition = string.Join(" and ", keyColumns
                .Select(x => builder.Table.Scope.ConnectionString.Escape(x))
                .Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
            WhenMatchedAction = columnsToUpdate.Count > 0
                ? "update set " + string.Join(",", columnsToUpdate
                    .Select(x => builder.Table.Scope.ConnectionString.Escape(x))
                    .Select(x => "t." + x + "=s." + x))
                : null,
        });
    }

    public static ResilientSqlTableTableFinalizerBuilder SimpleMergeInsertOnly(this ResilientSqlTableTableFinalizerBuilder builder, string[] keyColumns, int commandTimeout = 60 * 60)
    {
        return builder.Add(new CustomMsSqlMergeStatement(builder.Table.Scope.Context)
        {
            Name = "SimpleMergeInsertOnlyFinalizer",
            ConnectionString = builder.Table.Scope.ConnectionString,
            CommandTimeout = commandTimeout,
            SourceTableName = builder.Table.TempTableName,
            TargetTableName = builder.Table.TableName,
            SourceTableAlias = "s",
            TargetTableAlias = "t",
            OnCondition = string.Join(" and ", keyColumns
                .Select(x => builder.Table.Scope.ConnectionString.Escape(x))
                .Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
            WhenNotMatchedByTargetAction = "insert ("
                + string.Join(",", builder.Table.Columns
                    .Select(x => builder.Table.Scope.ConnectionString.Escape(x)))
                + ") values (" + string.Join(",", builder.Table.Columns
                    .Select(x => "s." + builder.Table.Scope.ConnectionString.Escape(x))) + ")",
        });
    }

    public static ResilientSqlScopeProcessBuilder CustomJob(this ResilientSqlScopeProcessBuilder builder, Action<CustomJob> action)
    {
        builder.Processes.Add(new CustomJob(builder.Scope.Context)
        {
            Action = action,
        });

        return builder;
    }

    public static ResilientSqlScopeProcessBuilder MsSqlDisableForeignKeys(this ResilientSqlScopeProcessBuilder builder)
    {
        if (builder.Scope.Tables.Count > 1)
        {
            builder.Processes.Add(new MsSqlDisableConstraintCheck(builder.Scope.Context)
            {
                Name = "disable foreign keys",
                ConnectionString = builder.Scope.ConnectionString,
                TableNames = builder.Scope.Tables.Select(x => x.TableName)
                    .Concat(builder.Scope.Tables.Where(x => x.AdditionalTables != null).SelectMany(x => x.AdditionalTables.Select(at => at.TableName)))
                    .ToArray(),
            });
        }

        return builder;
    }

    public static ResilientSqlScopeProcessBuilder MsSqlEnableForeignKeys(this ResilientSqlScopeProcessBuilder builder)
    {
        if (builder.Scope.Tables.Count > 1)
        {
            builder.Processes.Add(new MsSqlEnableConstraintCheck(builder.Scope.Context)
            {
                Name = "enable foreign keys",
                ConnectionString = builder.Scope.ConnectionString,
                TableNames = builder.Scope.Tables.Select(x => x.TableName)
                    .Concat(builder.Scope.Tables.Where(x => x.AdditionalTables != null).SelectMany(x => x.AdditionalTables.Select(at => at.TableName)))
                    .ToArray(),
            });
        }

        return builder;
    }
}
