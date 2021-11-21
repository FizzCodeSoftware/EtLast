namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Linq;

    public static class ResilientSqlFinalizerExtensions
    {
        public static IEnumerable<IExecutable> TruncateTargetTableFinalizer(this ResilientTableBase table, int commandTimeout = 60 * 60)
        {
            yield return new TruncateTable(table.Scope.Context, table.Topic, "TruncateTargetTableFinalizer")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                TableName = table.TableName,
                CommandTimeout = commandTimeout,
            };
        }

        public static IEnumerable<IExecutable> DeleteTargetTableFinalizer(this ResilientTableBase table, int commandTimeout = 60 * 60)
        {
            yield return new DeleteTable(table.Scope.Context, table.Topic, "DeleteTargetTableFinalizer")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                TableName = table.TableName,
                CommandTimeout = commandTimeout,
            };
        }

        public static IEnumerable<IExecutable> CopyTableFinalizer(this ResilientTableBase table, int commandTimeout = 60 * 60, bool copyIdentityColumns = false)
        {
            if (copyIdentityColumns && table.Columns == null)
                throw new EtlException(table.Scope, "identity columns can be copied only if the " + nameof(ResilientTable) + "." + nameof(ResilientTableBase.Columns) + " is specified");

#pragma warning disable RCS1227 // Validate arguments correctly.
            yield return new CopyTableIntoExistingTable(table.Scope.Context, table.Topic, "CopyTableFinalizer")
#pragma warning restore RCS1227 // Validate arguments correctly.
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = table.TempTableName,
                    TargetTableName = table.TableName,
                    ColumnConfiguration = table
                        .Columns?
                        .Select(x => new ColumnCopyConfiguration(x))
                        .ToList(),
                },
                CommandTimeout = commandTimeout,
                CopyIdentityColumns = copyIdentityColumns,
            };
        }

        public static IEnumerable<IExecutable> SimpleMsSqlMergeFinalizer(this ResilientTableBase table, string keyColumn, int commandTimeout = 60 * 60)
        {
            var columnsToUpdate = table.Columns
                .Where(c => !string.Equals(c, keyColumn, System.StringComparison.InvariantCultureIgnoreCase))
                .ToList();

            yield return new CustomMsSqlMergeStatement(table.Scope.Context, table.Topic, "SimpleMergeFinalizer")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                CommandTimeout = commandTimeout,
                SourceTableName = table.TempTableName,
                TargetTableName = table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = "((s." + keyColumn + "=t." + keyColumn + ") or (s." + keyColumn + " is null and t." + keyColumn + " is null))",
                WhenMatchedAction = columnsToUpdate.Count > 0
                    ? "update set " + string.Join(",", columnsToUpdate.Select(c => "t." + c + "=s." + c))
                    : null,
                WhenNotMatchedByTargetAction = "insert (" + string.Join(",", table.Columns) + ") values (" + string.Join(",", table.Columns.Select(c => "s." + c)) + ")",
            };
        }

        public static IEnumerable<IExecutable> SimpleMsSqlMergeFinalizer(this ResilientTableBase table, string[] keyColumns, int commandTimeout = 60 * 60)
        {
            var columnsToUpdate = table.Columns
                .Where(c => !keyColumns.Any(keyColumn => string.Equals(c, keyColumn, System.StringComparison.InvariantCultureIgnoreCase)))
                .ToList();

            yield return new CustomMsSqlMergeStatement(table.Scope.Context, table.Topic, "SimpleMergeFinalizer")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                CommandTimeout = commandTimeout,
                SourceTableName = table.TempTableName,
                TargetTableName = table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", keyColumns.Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
                WhenMatchedAction = columnsToUpdate.Count > 0
                    ? "update set " + string.Join(",", columnsToUpdate.Select(c => "t." + c + "=s." + c))
                    : null,
                WhenNotMatchedByTargetAction = "insert (" + string.Join(",", table.Columns) + ") values (" + string.Join(",", table.Columns.Select(c => "s." + c)) + ")",
            };
        }

        public static IEnumerable<IExecutable> SimpleMsSqlMergeUpdateOnlyFinalizer(this ResilientTableBase table, string[] keyColumns, int commandTimeout = 60 * 60)
        {
            var columnsToUpdate = table.Columns.Where(c => !keyColumns.Contains(c)).ToList();

            yield return new CustomMsSqlMergeStatement(table.Scope.Context, table.Topic, "SimpleMergeUpdateOnlyFinalizer")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                CommandTimeout = commandTimeout,
                SourceTableName = table.TempTableName,
                TargetTableName = table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", keyColumns.Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
                WhenMatchedAction = columnsToUpdate.Count > 0
                    ? "update set " + string.Join(",", columnsToUpdate.Select(c => "t." + c + "=s." + c))
                    : null,
            };
        }

        public static IEnumerable<IExecutable> SimpleMergeInsertOnlyFinalizer(this ResilientTableBase table, string[] keyColumns, int commandTimeout = 60 * 60)
        {
            yield return new CustomMsSqlMergeStatement(table.Scope.Context, table.Topic, "SimpleMergeInsertOnlyFinalizer")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                CommandTimeout = commandTimeout,
                SourceTableName = table.TempTableName,
                TargetTableName = table.TableName,
                SourceTableAlias = "s",
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", keyColumns.Select(x => "((s." + x + "=t." + x + ") or (s." + x + " is null and t." + x + " is null))")),
                WhenNotMatchedByTargetAction = "insert (" + string.Join(",", table.Columns) + ") values (" + string.Join(",", table.Columns.Select(c => "s." + c)) + ")",
            };
        }

        public static IEnumerable<IExecutable> MsSqlDisableForeignKeys(this ResilientSqlScope scope)
        {
            if (scope.Configuration.Tables.Count > 1)
            {
                yield return new MsSqlDisableConstraintCheck(scope.Context, null, "disable foreign keys")
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    TableNames = scope.Configuration.Tables.Select(x => x.TableName)
                        .Concat(scope.Configuration.Tables.Where(x => x.AdditionalTables != null).SelectMany(x => x.AdditionalTables.Select(at => at.TableName)))
                        .ToArray(),
                };
            }
        }

        public static IEnumerable<IExecutable> MsSqlEnableForeignKeys(this ResilientSqlScope scope)
        {
            if (scope.Configuration.Tables.Count > 1)
            {
                yield return new MsSqlEnableConstraintCheck(scope.Context, null, "enable foreign keys")
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    TableNames = scope.Configuration.Tables.Select(x => x.TableName)
                        .Concat(scope.Configuration.Tables.Where(x => x.AdditionalTables != null).SelectMany(x => x.AdditionalTables.Select(at => at.TableName)))
                        .ToArray(),
                };
            }
        }
    }
}