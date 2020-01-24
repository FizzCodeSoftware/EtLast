namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Linq;

    public class ResilientSqlScope : AbstractExecutableProcess
    {
        private ResilientSqlScopeConfiguration _configuration;

        public ResilientSqlScopeConfiguration Configuration
        {
            get => _configuration;
            set
            {
                if (_configuration != null)
                {
                    _configuration.Scope = null;
                    foreach (var table in _configuration.Tables)
                    {
                        table.Scope = null;
                    }
                }

                _configuration = value;
                _configuration.Scope = this;

                foreach (var table in _configuration.Tables)
                {
                    table.Scope = this;
                }
            }
        }

        public ResilientSqlScope(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
        {
            if (Configuration == null)
                throw new ProcessParameterNullException(this, nameof(Configuration));

            if (Configuration.Tables == null)
                throw new ProcessParameterNullException(this, nameof(Configuration.Tables));

            if (Configuration.ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(Configuration.ConnectionString));
        }

        protected override void ExecuteImpl()
        {
            Context.Log(LogSeverity.Information, this, "scope started");

            var maxRetryCount = Configuration.FinalizerRetryCount;
            if (Configuration.FinalizerTransactionScopeKind != TransactionScopeKind.RequiresNew && maxRetryCount > 0)
                throw new InvalidProcessParameterException(this, nameof(Configuration.FinalizerRetryCount), null, "retrying finalizers can be possible only if the " + nameof(Configuration.FinalizerTransactionScopeKind) + " is set to " + nameof(TransactionScopeKind.RequiresNew));

            var initialExceptionCount = Context.ExceptionCount;
            var success = false;

            foreach (var table in Configuration.Tables)
            {
                if (string.IsNullOrEmpty(table.TempTableName))
                {
                    if (string.IsNullOrEmpty(Configuration.AutoTempTablePrefix) && string.IsNullOrEmpty(Configuration.AutoTempTablePostfix))
                        throw new InvalidProcessParameterException(this, nameof(table.TempTableName), null, nameof(ResilientTable) + "." + nameof(ResilientTableBase.TempTableName) + " must be specified if there is no " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePrefix) + " or " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePostfix) + " specified (table name: " + table.TableName + ")");

                    table.TempTableName = Configuration.AutoTempTablePrefix + table.TableName + Configuration.AutoTempTablePostfix;
                }

                if (string.IsNullOrEmpty(table.TableName))
                    throw new ProcessParameterNullException(this, nameof(ResilientTableBase.TableName));

                if (table.MainProcessCreator == null && table.PartitionedMainProcessCreator == null)
                    throw new InvalidProcessParameterException(this, nameof(ResilientTable.MainProcessCreator) + "/" + nameof(ResilientTable.PartitionedMainProcessCreator), null, nameof(ResilientTable.MainProcessCreator) + " or " + nameof(ResilientTable.PartitionedMainProcessCreator) + " must be supplied for " + table.TableName);

                if (table.MainProcessCreator != null && table.PartitionedMainProcessCreator != null)
                    throw new InvalidProcessParameterException(this, nameof(ResilientTable.MainProcessCreator) + "/" + nameof(ResilientTable.PartitionedMainProcessCreator), null, "only one of " + nameof(ResilientTable.MainProcessCreator) + " or " + nameof(ResilientTable.PartitionedMainProcessCreator) + " can be supplied for " + table.TableName);

                if (table.FinalizerCreator == null)
                    throw new ProcessParameterNullException(this, nameof(ResilientTable.FinalizerCreator));
            }

            try
            {
                CreateTempTables(Context);

                if (Context.ExceptionCount > initialExceptionCount)
                    return;

                if (Configuration.InitializerCreator != null)
                {
                    var initializationSuccessful = false;
                    Initialize(maxRetryCount, ref initialExceptionCount, ref initializationSuccessful);

                    if (!initializationSuccessful)
                    {
                        Context.Log(LogSeverity.Information, this, "initialization failed after {Elapsed}", LastInvocation.Elapsed);
                        return;
                    }
                }

                foreach (var table in Configuration.Tables)
                {
                    for (var partitionIndex = 0; ; partitionIndex++)
                    {
                        var creatorScopeKind = table.SuppressTransactionScopeForCreators
                            ? TransactionScopeKind.Suppress
                            : TransactionScopeKind.None;

                        if (table.MainProcessCreator != null)
                        {
                            Context.Log(LogSeverity.Information, this, "creating main process for table {TableName}",
                                Configuration.ConnectionString.Unescape(table.TableName));

                            IExecutable[] mainProcessList;

                            using (var creatorScope = Context.BeginScope(this, null, creatorScopeKind, LogSeverity.Information))
                            {
                                mainProcessList = table.MainProcessCreator
                                    .Invoke(table)
                                    .Where(x => x != null)
                                    .ToArray();
                            }

                            foreach (var process in mainProcessList)
                            {
                                var preExceptionCount = Context.ExceptionCount;
                                process.Execute(this);
                                if (Context.ExceptionCount > initialExceptionCount)
                                {
                                    return;
                                }
                            }

                            break;
                        }

                        Context.Log(LogSeverity.Information, this, "creating main process for table {TableName}, (partition #{PartitionIndex})",
                            Configuration.ConnectionString.Unescape(table.TableName), partitionIndex);

                        IEvaluable mainEvaluableProcess;

                        using (var creatorScope = Context.BeginScope(this, null, creatorScopeKind, LogSeverity.Information))
                        {
                            mainEvaluableProcess = table.PartitionedMainProcessCreator.Invoke(table, partitionIndex);
                        }

                        var rowCount = 0;
                        foreach (var row in mainEvaluableProcess.Evaluate()) // must enumerate through all rows
                        {
                            rowCount++;
                        }

                        if (Context.ExceptionCount > initialExceptionCount)
                            return;

                        if (rowCount == 0)
                            break;
                    }
                }

                for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
                {
                    Context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", retryCounter);
                    using (var scope = Context.BeginScope(this, null, Configuration.FinalizerTransactionScopeKind, LogSeverity.Information))
                    {
                        if (Configuration.PreFinalizerCreator != null)
                        {
                            var preFinalizer = new ResilientSqlScopePreFinalizerManager(this);
                            preFinalizer.Execute();
                        }

                        if (Context.ExceptionCount == initialExceptionCount)
                        {
                            var tableFinalizer = new ResilientTableFinalizerManager(this);
                            tableFinalizer.Execute();

                            if (Configuration.PostFinalizerCreator != null && Context.ExceptionCount == initialExceptionCount)
                            {
                                var postFinalizer = new ResilientSqlScopePostFinalizerManager(this);
                                postFinalizer.Execute();
                            }
                        }

                        var currentExceptionCount = Context.ExceptionCount;
                        if (currentExceptionCount == initialExceptionCount)
                        {
                            scope.Complete();

                            success = true;
                            break;
                        }

                        initialExceptionCount = currentExceptionCount;
                    }
                }
            }
            finally
            {
                if (Configuration.TempTableMode != ResilientSqlScopeTempTableMode.AlwaysKeep)
                {
                    if (success || Configuration.TempTableMode == ResilientSqlScopeTempTableMode.AlwaysDrop)
                    {
                        DropTempTables(Context);
                    }
                }
            }

            Context.Log(LogSeverity.Information, this, success ? "finished in {Elapsed}" : "failed after {Elapsed}", LastInvocation.Elapsed);
        }

        private void Initialize(int maxRetryCount, ref int initialExceptionCount, ref bool initializationSuccessful)
        {
            for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
            {
                Context.Log(LogSeverity.Information, this, "initialization round {InitializationRound} started", retryCounter);
                using (var scope = Context.BeginScope(this, null, Configuration.InitializationTransactionScopeKind, LogSeverity.Information))
                {
                    var manager = new ResilientSqlScopeInitializerManager(this);
                    manager.Execute();

                    var currentExceptionCount = Context.ExceptionCount;
                    if (currentExceptionCount == initialExceptionCount)
                    {
                        scope.Complete();

                        initializationSuccessful = true;
                        break;
                    }

                    initialExceptionCount = currentExceptionCount;
                }
            }
        }

        public static IEnumerable<IExecutable> DeleteTargetTableFinalizer(ResilientTableBase table, int commandTimeout = 60)
        {
            yield return new DeleteTableProcess(table.Scope.Context, "DeleteContentFromTargetTable")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                TableName = table.TableName,
                CommandTimeout = commandTimeout,
            };
        }

        public static IEnumerable<IExecutable> CopyTableFinalizer(ResilientTableBase table, int commandTimeout = 60, bool copyIdentityColumns = false)
        {
            if (copyIdentityColumns && table.Columns == null)
                throw new EtlException(table.Scope, "identity columns can be copied only if the " + nameof(ResilientTable) + "." + nameof(ResilientTableBase.Columns) + " is specified");

#pragma warning disable RCS1227 // Validate arguments correctly.
            yield return new CopyTableIntoExistingTableProcess(table.Scope.Context, "CopyTempToTargetTable")
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

        public static IEnumerable<IExecutable> SimpleMergeFinalizer(ResilientTableBase table, string[] keyColumns, int commandTimeout = 60)
        {
            yield return new CustomMsSqlMergeSqlStatementProcess(table.Scope.Context)
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                CommandTimeout = commandTimeout,
                SourceTableName = table.TempTableName,
                SourceTableAlias = "s",
                TargetTableName = table.TableName,
                TargetTableAlias = "t",
                OnCondition = string.Join(" and ", keyColumns.Select(x => "s." + x + "=t." + x)),
                WhenMatchedAction = "update set " + string.Join(",", table.Columns.Where(c => !keyColumns.Contains(c)).Select(c => "t." + c + "=s." + c)),
                WhenNotMatchedByTargetAction = "insert (" + string.Join(",", table.Columns) + ") values (" + string.Join(",", table.Columns.Select(c => "s." + c)) + ")",
            };
        }

        private void CreateTempTables(IEtlContext context)
        {
            var config = new List<TableCopyConfiguration>();
            foreach (var table in Configuration.Tables)
            {
                config.Add(new TableCopyConfiguration()
                {
                    SourceTableName = table.TableName,
                    TargetTableName = table.TempTableName,
                    ColumnConfiguration = table
                        .Columns?
                        .Select(x => new ColumnCopyConfiguration(x))
                        .ToList(),
                });

                if (table.AdditionalTables != null)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        config.Add(new TableCopyConfiguration()
                        {
                            SourceTableName = additionalTable.TableName,
                            TargetTableName = additionalTable.TempTableName,
                            ColumnConfiguration = additionalTable
                                .Columns?
                                .Select(x => new ColumnCopyConfiguration(x))
                                .ToList(),
                        });
                    }
                }
            }

            new CopyTableStructureProcess(context, "RecreateTempTables")
            {
                ConnectionString = Configuration.ConnectionString,
                SuppressExistingTransactionScope = true,
                Configuration = config,
            }.Execute(this);
        }

        private void DropTempTables(IEtlContext context)
        {
            var tempTableNames = Configuration.Tables
                .Select(x => x.TempTableName);

            var additionalTempTableNames = Configuration.Tables
                .Where(x => x.AdditionalTables != null)
                .SelectMany(x => x.AdditionalTables.Values.Select(y => y.TempTableName));

            new DropTablesProcess(context, "DropTempTables")
            {
                ConnectionString = Configuration.ConnectionString,
                TableNames = tempTableNames
                    .Concat(additionalTempTableNames)
                    .ToArray(),
            }.Execute(this);
        }
    }
}