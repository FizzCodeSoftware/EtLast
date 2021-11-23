namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Linq;

    public sealed class ResilientSqlScope : AbstractExecutable, IScope
    {
        private ResilientSqlScopeConfiguration _configuration;

        public ResilientSqlScopeConfiguration Configuration
        {
            get => _configuration;
            init
            {
                if (_configuration != null)
                {
                    _configuration.Scope = null;
                    foreach (var table in _configuration.Tables)
                    {
                        table.Scope = null;
                        if (table.AdditionalTables != null)
                        {
                            foreach (var additionalTable in table.AdditionalTables)
                            {
                                additionalTable.Scope = null;
                            }
                        }
                    }
                }

                _configuration = value;
                _configuration.Scope = this;

                foreach (var table in _configuration.Tables)
                {
                    table.Scope = this;
                    if (table.AdditionalTables != null)
                    {
                        foreach (var additionalTable in table.AdditionalTables)
                        {
                            additionalTable.Scope = this;
                        }
                    }
                }
            }
        }

        public ResilientSqlScope(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateImpl()
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
            var maxRetryCount = Configuration.FinalizerRetryCount;
            if (Configuration.FinalizerTransactionScopeKind != TransactionScopeKind.RequiresNew && maxRetryCount > 0)
                throw new InvalidProcessParameterException(this, nameof(Configuration.FinalizerRetryCount), null, "retrying finalizers can be possible only if the " + nameof(Configuration.FinalizerTransactionScopeKind) + " is set to " + nameof(TransactionScopeKind.RequiresNew));

            var initialExceptionCount = Context.ExceptionCount;
            var success = false;

            foreach (var table in Configuration.Tables)
            {
                if (string.IsNullOrEmpty(table.TempTableName)
                    && string.IsNullOrEmpty(Configuration.AutoTempTablePrefix)
                    && string.IsNullOrEmpty(Configuration.AutoTempTablePostfix))
                {
                    throw new InvalidProcessParameterException(this, nameof(table.TempTableName), null, nameof(ResilientTable) + "." + nameof(ResilientTableBase.TempTableName) + " must be specified if there is no " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePrefix) + " or " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePostfix) + " specified (table name: " + table.TableName + ")");
                }

                if (string.IsNullOrEmpty(table.TableName))
                    throw new ProcessParameterNullException(this, nameof(ResilientTableBase.TableName));

                if (table.MainProcessCreator == null && table.PartitionedMainProcessCreator == null)
                    throw new InvalidProcessParameterException(this, nameof(ResilientTable.MainProcessCreator) + "/" + nameof(ResilientTable.PartitionedMainProcessCreator), null, nameof(ResilientTable.MainProcessCreator) + " or " + nameof(ResilientTable.PartitionedMainProcessCreator) + " must be supplied for " + table.TableName);

                if (table.MainProcessCreator != null && table.PartitionedMainProcessCreator != null)
                    throw new InvalidProcessParameterException(this, nameof(ResilientTable.MainProcessCreator) + "/" + nameof(ResilientTable.PartitionedMainProcessCreator), null, "only one of " + nameof(ResilientTable.MainProcessCreator) + " or " + nameof(ResilientTable.PartitionedMainProcessCreator) + " can be supplied for " + table.TableName);

                if (table.Finalizers == null)
                    throw new ProcessParameterNullException(this, nameof(ResilientTable.Finalizers));

                if (table.AdditionalTables != null)
                {
                    foreach (var additionalTable in table.AdditionalTables)
                    {
                        if (additionalTable.Finalizers == null)
                            throw new ProcessParameterNullException(this, nameof(ResilientTable.Finalizers));
                    }
                }
            }

            try
            {
                CreateTempTables();

                if (Context.ExceptionCount > initialExceptionCount)
                    return;

                if (Configuration.Initializers != null)
                {
                    var initializationSuccessful = false;
                    Initialize(maxRetryCount, ref initialExceptionCount, ref initializationSuccessful);

                    if (!initializationSuccessful)
                    {
                        Context.Log(LogSeverity.Information, this, "initialization failed after {Elapsed}", InvocationInfo.LastInvocationStarted.Elapsed);
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
                            Context.Log(LogSeverity.Information, this, "processing table {TableName}",
                                Configuration.ConnectionString.Unescape(table.TableName));

                            IExecutable[] mainProcessList;

                            using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
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

                        Context.Log(LogSeverity.Information, this, "processing table {TableName}, (partition #{PartitionIndex})",
                            Configuration.ConnectionString.Unescape(table.TableName), partitionIndex);

                        IProducer mainEvaluableProcess;

                        using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
                        {
                            mainEvaluableProcess = table.PartitionedMainProcessCreator.Invoke(table, partitionIndex);
                        }

                        var rowCount = mainEvaluableProcess.Evaluate(this).CountRowsWithoutTransfer();

                        if (Context.ExceptionCount > initialExceptionCount)
                            return;

                        if (rowCount == 0)
                            break;
                    }
                }

                for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
                {
                    Context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", retryCounter);
                    using (var scope = Context.BeginScope(this, Configuration.FinalizerTransactionScopeKind, LogSeverity.Information))
                    {
                        if (Configuration.PreFinalizers != null)
                        {
                            IExecutable[] finalizers;

                            using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
                            {
                                var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
                                Configuration.PreFinalizers.Invoke(builder);
                                finalizers = builder.Processes.Where(x => x != null).ToArray();
                            }

                            if (finalizers?.Length > 0)
                            {
                                Context.Log(LogSeverity.Debug, this, "created {PreFinalizerCount} pre-finalizer(s)",
                                    finalizers?.Length ?? 0);

                                foreach (var finalizer in finalizers)
                                {
                                    var preExceptionCount = Context.ExceptionCount;

                                    Context.Log(LogSeverity.Information, this, "starting pre-finalizer: {Process}",
                                        finalizer.Name);

                                    finalizer.Execute(this);
                                    if (Context.ExceptionCount > preExceptionCount)
                                        break;
                                }
                            }
                        }

                        if (Context.ExceptionCount == initialExceptionCount)
                        {
                            var tablesOrderedTemp = new List<TableWithOrder>();
                            for (var i = 0; i < Configuration.Tables.Count; i++)
                            {
                                tablesOrderedTemp.Add(new TableWithOrder()
                                {
                                    Table = Configuration.Tables[i],
                                    OriginalIndex = i,
                                });
                            }

                            var tablesOrdered = tablesOrderedTemp
                                .OrderBy(x => x.Table.OrderDuringFinalization)
                                .ThenBy(x => x.OriginalIndex)
                                .Select(x => x.Table)
                                .ToList();

                            var recordCounts = new int[tablesOrdered.Count];
                            for (var i = 0; i < tablesOrdered.Count; i++)
                            {
                                var table = tablesOrdered[i];

                                var recordCount = CountTempRecordsIn(table);
                                if (table.AdditionalTables?.Count > 0)
                                {
                                    foreach (var additionalTable in table.AdditionalTables)
                                    {
                                        recordCount += CountTempRecordsIn(additionalTable);
                                    }
                                }

                                recordCounts[i] = recordCount;
                            }

                            Context.Log(LogSeverity.Information, this, "{TableCountWithData} of {TotalTableCount} temp table contains data",
                                recordCounts.Count(x => x > 0), recordCounts.Length);

                            for (var i = 0; i < tablesOrdered.Count; i++)
                            {
                                var table = tablesOrdered[i];
                                if (table.SkipFinalizersIfNoTempData && recordCounts[i] == 0)
                                {
                                    Context.Log(LogSeverity.Debug, this, "no data found for {TableName}, skipping finalizers",
                                        Configuration.ConnectionString.Unescape(table.TableName));

                                    continue;
                                }

                                var creatorScopeKind = table.SuppressTransactionScopeForCreators
                                    ? TransactionScopeKind.Suppress
                                    : TransactionScopeKind.None;

                                var allFinalizers = new Dictionary<string, IExecutable[]>();
                                using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
                                {
                                    var builder = new ResilientSqlTableTableFinalizerBuilder() { Table = table };
                                    table.Finalizers.Invoke(builder);
                                    var finalizers = builder.Finalizers.Where(x => x != null).ToArray();

                                    allFinalizers[table.TableName] = finalizers;

                                    Context.Log(LogSeverity.Debug, this, "created {FinalizerCount} finalizer(s) for {TableName}",
                                        finalizers.Length,
                                        Configuration.ConnectionString.Unescape(table.TableName));

                                    if (table.AdditionalTables != null)
                                    {
                                        foreach (var additionalTable in table.AdditionalTables)
                                        {
                                            builder = new ResilientSqlTableTableFinalizerBuilder() { Table = additionalTable };
                                            additionalTable.Finalizers.Invoke(builder);
                                            finalizers = builder.Finalizers.Where(x => x != null).ToArray();

                                            allFinalizers[additionalTable.TableName] = finalizers;

                                            Context.Log(LogSeverity.Debug, this, "created {FinalizerCount} finalizer(s) for {TableName}",
                                                finalizers.Length,
                                                Configuration.ConnectionString.Unescape(additionalTable.TableName));
                                        }
                                    }
                                }

                                foreach (var tableFinalizers in allFinalizers)
                                {
                                    foreach (var finalizer in tableFinalizers.Value)
                                    {
                                        var preExceptionCount = Context.ExceptionCount;
                                        Context.Log(LogSeverity.Information, this, "finalizing {TableName} with {Process}",
                                            Configuration.ConnectionString.Unescape(tableFinalizers.Key),
                                            finalizer.Name);

                                        finalizer.Execute(this);
                                        if (Context.ExceptionCount > preExceptionCount)
                                        {
                                            break;
                                        }
                                    }
                                }
                            }

                            if (Configuration.PostFinalizers != null && Context.ExceptionCount == initialExceptionCount)
                            {
                                IExecutable[] finalizers;

                                using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
                                {
                                    var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
                                    Configuration.PostFinalizers.Invoke(builder);
                                    finalizers = builder.Processes.Where(x => x != null).ToArray();
                                }

                                if (finalizers?.Length > 0)
                                {
                                    Context.Log(LogSeverity.Debug, this, "created {PostFinalizerCount} post-finalizer(s)",
                                        finalizers?.Length ?? 0);

                                    foreach (var finalizer in finalizers)
                                    {
                                        var preExceptionCount = Context.ExceptionCount;

                                        Context.Log(LogSeverity.Information, this, "starting post-finalizer: {Process}",
                                            finalizer.Name);

                                        finalizer.Execute(this);
                                        if (Context.ExceptionCount > preExceptionCount)
                                            break;
                                    }
                                }
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
                        DropTempTables();
                    }
                }
            }

            Context.Log(LogSeverity.Information, this, success ? "finished in {Elapsed}" : "failed after {Elapsed}", InvocationInfo.LastInvocationStarted.Elapsed);
        }

        private int CountTempRecordsIn(ResilientTableBase table)
        {
            var count = new GetTableRecordCount(Context)
            {
                Name = "TempRecordCountReader",
                ConnectionString = Configuration.ConnectionString,
                TableName = table.TempTableName,
            }.Execute(this);

            return count;
        }

        private void Initialize(int maxRetryCount, ref int initialExceptionCount, ref bool initializationSuccessful)
        {
            for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
            {
                Context.Log(LogSeverity.Information, this, "initialization round {InitializationRound} started", retryCounter);
                using (var scope = Context.BeginScope(this, Configuration.InitializationTransactionScopeKind, LogSeverity.Information))
                {
                    IExecutable[] initializers;

                    using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
                    {
                        var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
                        Configuration.Initializers.Invoke(builder);
                        initializers = builder.Processes.Where(x => x != null).ToArray();

                        Context.Log(LogSeverity.Information, this, "created {InitializerCount} initializers", initializers?.Length ?? 0);
                    }

                    if (initializers?.Length > 0)
                    {
                        Context.Log(LogSeverity.Information, this, "starting initializers");

                        foreach (var initializer in initializers)
                        {
                            var preExceptionCount = Context.ExceptionCount;
                            initializer.Execute(this);
                            if (Context.ExceptionCount > preExceptionCount)
                                break;
                        }
                    }

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

        private void CreateTempTables()
        {
            var config = new List<TableCopyConfiguration>();
            foreach (var table in Configuration.Tables)
            {
                config.Add(new TableCopyConfiguration()
                {
                    SourceTableName = table.TableName,
                    TargetTableName = table.TempTableName,
                    ColumnConfiguration = table.Columns?
                        .Select(c => new ColumnCopyConfiguration(c))
                        .ToList(),
                });

                if (table.AdditionalTables != null)
                {
                    foreach (var additionalTable in table.AdditionalTables)
                    {
                        config.Add(new TableCopyConfiguration()
                        {
                            SourceTableName = additionalTable.TableName,
                            TargetTableName = additionalTable.TempTableName,
                            ColumnConfiguration = additionalTable.Columns?
                                .Select(c => new ColumnCopyConfiguration(c))
                                .ToList(),
                        });
                    }
                }
            }

            new CopyTableStructure(Context)
            {
                Name = "RecreateTempTables",
                ConnectionString = Configuration.ConnectionString,
                SuppressExistingTransactionScope = true,
                Configuration = config,
            }.Execute(this);
        }

        private void DropTempTables()
        {
            var tempTableNames = Configuration.Tables
                .Select(x => x.TempTableName);

            var additionalTempTableNames = Configuration.Tables
                .Where(x => x.AdditionalTables != null)
                .SelectMany(x => x.AdditionalTables.Select(y => y.TempTableName));

            new DropTables(Context)
            {
                Name = "DropTempTables",
                ConnectionString = Configuration.ConnectionString,
                TableNames = tempTableNames
                    .Concat(additionalTempTableNames)
                    .ToArray(),
            }.Execute(this);
        }

        private class TableWithOrder
        {
            internal ResilientTable Table { get; set; }
            internal int OriginalIndex { get; set; }
        }
    }
}