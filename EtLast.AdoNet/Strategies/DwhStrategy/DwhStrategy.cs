namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;

    public class DwhStrategy : IEtlStrategy
    {
        public ICaller Caller { get; private set; }
        public string InstanceName { get; set; }
        public string Name => InstanceName ?? TypeHelpers.GetFriendlyTypeName(GetType());

        private DwhStrategyConfiguration _configuration;

        public DwhStrategyConfiguration Configuration
        {
            get => _configuration;
            set
            {
                if (_configuration != null)
                    _configuration.Strategy = null;

                _configuration = value;
                _configuration.Strategy = this;

                foreach (var table in _configuration.Tables)
                {
                    table.Strategy = this;
                }
            }
        }

        public void Execute(ICaller caller, IEtlContext context)
        {
            Caller = caller;
            context.Log(LogSeverity.Information, this, "strategy started (dwh)");
            var startedOn = Stopwatch.StartNew();

            if (Configuration == null)
                throw new StrategyParameterNullException(this, nameof(Configuration));

            if (Configuration.Tables == null)
                throw new StrategyParameterNullException(this, nameof(Configuration.Tables));

            if (Configuration.ConnectionStringKey == null)
                throw new StrategyParameterNullException(this, nameof(Configuration.ConnectionStringKey));

            var maxRetryCount = Configuration.FinalizerRetryCount;
            if (Configuration.FinalizerTransactionScopeKind != TransactionScopeKind.RequiresNew && maxRetryCount > 0)
                throw new InvalidStrategyParameterException(this, nameof(Configuration.FinalizerRetryCount), null, "retrying finalizers can be possible only if the " + nameof(Configuration.FinalizerTransactionScopeKind) + " is set to " + nameof(TransactionScopeKind.RequiresNew));

            var initialExceptionCount = context.GetExceptions().Count;
            var success = false;

            foreach (var table in Configuration.Tables)
            {
                if (string.IsNullOrEmpty(table.TempTableName))
                {
                    if (string.IsNullOrEmpty(Configuration.AutoTempTablePrefix) && string.IsNullOrEmpty(Configuration.AutoTempTablePostfix))
                        throw new InvalidStrategyParameterException(this, nameof(table.TempTableName), null, nameof(DwhStrategyTable) + "." + nameof(DwhStrategyTableBase.TempTableName) + " must be specified if there is no " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePrefix) + " or " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePostfix) + " specified (table name: " + table.TableName + ")");

                    table.TempTableName = Configuration.AutoTempTablePrefix + table.TableName + Configuration.AutoTempTablePostfix;
                }

                if (string.IsNullOrEmpty(table.TableName))
                    throw new StrategyParameterNullException(this, nameof(DwhStrategyTableBase.TableName));

                if (table.MainProcessCreator == null && table.PartitionedMainProcessCreator == null)
                    throw new InvalidStrategyParameterException(this, nameof(DwhStrategyTable.MainProcessCreator) + "/" + nameof(DwhStrategyTable.PartitionedMainProcessCreator), null, nameof(DwhStrategyTable.MainProcessCreator) + " or " + nameof(DwhStrategyTable.PartitionedMainProcessCreator) + " must be supplied for " + table.TableName);

                if (table.MainProcessCreator != null && table.PartitionedMainProcessCreator != null)
                    throw new InvalidStrategyParameterException(this, nameof(DwhStrategyTable.MainProcessCreator) + "/" + nameof(DwhStrategyTable.PartitionedMainProcessCreator), null, "only one of " + nameof(DwhStrategyTable.MainProcessCreator) + " or " + nameof(DwhStrategyTable.PartitionedMainProcessCreator) + " can be supplied for " + table.TableName);

                if (table.FinalizerJobsCreator == null)
                    throw new StrategyParameterNullException(this, nameof(DwhStrategyTable.FinalizerJobsCreator));
            }

            try
            {
                CreateTempTables(context);

                if (context.GetExceptions().Count > initialExceptionCount)
                    return;

                foreach (var table in Configuration.Tables)
                {
                    for (var partitionIndex = 0; ; partitionIndex++)
                    {
                        IFinalProcess mainProcess;

                        var creatorScopeKind = table.SuppressTransactionScopeForCreators
                            ? TransactionScopeKind.Suppress
                            : TransactionScopeKind.None;

                        if (table.MainProcessCreator != null)
                        {
                            context.Log(LogSeverity.Information, this, "creating main process for table {TableName}",
                                Helpers.UnEscapeTableName(table.TableName));

                            using (var creatorScope = context.BeginScope(this, null, null, creatorScopeKind, LogSeverity.Information))
                            {
                                mainProcess = table.MainProcessCreator.Invoke(table);
                            }

                            mainProcess.EvaluateWithoutResult(this);

                            if (context.GetExceptions().Count > initialExceptionCount)
                                return;

                            break;
                        }

                        context.Log(LogSeverity.Information, this, "creating main process for table {TableName}, (partition #{PartitionIndex})",
                            Helpers.UnEscapeTableName(table.TableName), partitionIndex);

                        using (var creatorScope = context.BeginScope(this, null, null, creatorScopeKind, LogSeverity.Information))
                        {
                            mainProcess = table.PartitionedMainProcessCreator.Invoke(table, partitionIndex);
                        }

                        mainProcess.Name += "/#" + (partitionIndex + 1).ToString("D", CultureInfo.InvariantCulture);
                        var rowCount = 0;
                        foreach (var row in mainProcess.Evaluate()) // must enumerate through all rows
                        {
                            rowCount++;
                        }

                        if (context.GetExceptions().Count > initialExceptionCount)
                            return;

                        if (rowCount == 0)
                            break;
                    }
                }

                for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
                {
                    context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", retryCounter);
                    using (var scope = context.BeginScope(this, null, null, Configuration.FinalizerTransactionScopeKind, LogSeverity.Information))
                    {
                        if (Configuration.BeforeFinalizersJobCreator != null)
                        {
                            var preFinalizer = new DwhStrategyPreFinalizerManager();
                            preFinalizer.Execute(context, this);
                        }

                        var tableFinalizer = new DwhStrategyTableFinalizerManager();
                        tableFinalizer.Execute(context, this);

                        if (Configuration.AfterFinalizersJobCreator != null)
                        {
                            var postFinalizer = new DwhStrategyPostFinalizerManager();
                            postFinalizer.Execute(context, this);
                        }

                        var currentExceptionCount = context.GetExceptions().Count;
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
                if (Configuration.TempTableMode != DwhStrategyTempTableMode.AlwaysKeep)
                {
                    if (success || Configuration.TempTableMode == DwhStrategyTempTableMode.AlwaysDrop)
                    {
                        DropTempTables(context);
                    }
                }
            }

            context.Log(LogSeverity.Information, this, success ? "finished in {Elapsed}" : "failed after {Elapsed}", startedOn.Elapsed);
        }

        public static IJob DeleteTargetTableFinalizer(DwhStrategyTableBase table)
        {
            return new DeleteTableJob
            {
                InstanceName = "DeleteContentFromTargetTable",
                ConnectionStringKey = table.Strategy.Configuration.ConnectionStringKey,
                TableName = table.TableName,
            };
        }

        public static IJob CopyTableFinalizer(DwhStrategyTableBase table, int commandTimeout, bool copyIdentityColumns = false)
        {
            return new CopyTableIntoExistingTableJob
            {
                InstanceName = "CopyTempToTargetTable",
                ConnectionStringKey = table.Strategy.Configuration.ConnectionStringKey,
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

            var process = new JobHostProcess(context, "RecreateTempTables");
            process.AddJob(new CopyTableStructureJob
            {
                InstanceName = "RecreateTempTables",
                ConnectionStringKey = Configuration.ConnectionStringKey,
                SuppressExistingTransactionScope = true,
                Configuration = config,
            });

            process.EvaluateWithoutResult(this);
        }

        private void DropTempTables(IEtlContext context)
        {
            var tempTableNames = Configuration.Tables
                .Select(x => x.TempTableName);

            var additionalTempTableNames = Configuration.Tables
                .Where(x => x.AdditionalTables != null)
                .SelectMany(x => x.AdditionalTables.Values.Select(y => y.TempTableName));

            var process = new JobHostProcess(context, "DropTempTablesProcess");
            process.AddJob(new DropTablesJob()
            {
                InstanceName = "DropTempTables",
                ConnectionStringKey = Configuration.ConnectionStringKey,
                TableNames = tempTableNames
                    .Concat(additionalTempTableNames)
                    .ToArray(),
            });

            process.EvaluateWithoutResult(this);
        }
    }
}