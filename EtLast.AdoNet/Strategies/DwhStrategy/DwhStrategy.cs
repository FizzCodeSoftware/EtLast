namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    public class DwhStrategy : IEtlStrategy
    {
        public ICaller Caller { get; private set; }
        public string InstanceName { get; set; }
        public string Name => InstanceName ?? GetType().Name;

        public DwhStrategyConfiguration Configuration { get; }

        public DwhStrategy(DwhStrategyConfiguration configuration)
        {
            Configuration = configuration;
        }

        public void Execute(ICaller caller, IEtlContext context)
        {
            Caller = caller;
            context.Log(LogSeverity.Information, this, "started");

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
                        throw new InvalidStrategyParameterException(this, nameof(table.TempTableName), null, nameof(DwhStrategyTableConfiguration) + "." + nameof(DwhStrategyTableConfigurationBase.TempTableName) + " must be specified if there is no " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePrefix) + " or " + nameof(Configuration) + "." + nameof(Configuration.AutoTempTablePostfix) + " specified (table name: " + table.TableName + ")");

                    table.TempTableName = Configuration.AutoTempTablePrefix + table.TableName + Configuration.AutoTempTablePostfix;
                }

                if (string.IsNullOrEmpty(table.TableName))
                    throw new StrategyParameterNullException(this, nameof(DwhStrategyTableConfigurationBase.TableName));

                if (table.MainProcessCreator == null && table.PartitionedMainProcessCreator == null)
                    throw new InvalidStrategyParameterException(this, nameof(DwhStrategyTableConfiguration.MainProcessCreator) + "/" + nameof(DwhStrategyTableConfiguration.PartitionedMainProcessCreator), null, nameof(DwhStrategyTableConfiguration.MainProcessCreator) + " or " + nameof(DwhStrategyTableConfiguration.PartitionedMainProcessCreator) + " must be supplied for " + table.TableName);

                if (table.MainProcessCreator != null && table.PartitionedMainProcessCreator != null)
                    throw new InvalidStrategyParameterException(this, nameof(DwhStrategyTableConfiguration.MainProcessCreator) + "/" + nameof(DwhStrategyTableConfiguration.PartitionedMainProcessCreator), null, "only one of " + nameof(DwhStrategyTableConfiguration.MainProcessCreator) + " or " + nameof(DwhStrategyTableConfiguration.PartitionedMainProcessCreator) + " can be supplied for " + table.TableName);

                if (table.FinalizerJobsCreator == null)
                    throw new StrategyParameterNullException(this, nameof(DwhStrategyTableConfiguration.FinalizerJobsCreator));
            }

            try
            {
                CreateTempTables(context);

                if (context.GetExceptions().Count > initialExceptionCount)
                    return;

                context.Log(LogSeverity.Information, this, "processing tables");

                foreach (var table in Configuration.Tables)
                {
                    if (table.MainProcessCreator != null)
                        context.Log(LogSeverity.Information, this, "processing {TableName}", Helpers.UnEscapeTableName(table.TableName));

                    for (var partitionIndex = 0; ; partitionIndex++)
                    {
                        if (table.PartitionedMainProcessCreator != null)
                            context.Log(LogSeverity.Information, this, "processing {TableName} (partition #{PartitionIndex})", Helpers.UnEscapeTableName(table.TableName), partitionIndex);

                        IFinalProcess mainProcess;

                        var creatorScopeKind = table.SuppressTransactionScopeForCreators
                            ? TransactionScopeKind.Suppress
                            : TransactionScopeKind.None;

                        if (table.MainProcessCreator != null)
                        {
                            using (var creatorScope = context.BeginScope(creatorScopeKind))
                            {
                                mainProcess = table.MainProcessCreator.Invoke(Configuration.ConnectionStringKey, table);
                            }

                            mainProcess.EvaluateWithoutResult(this);

                            if (context.GetExceptions().Count > initialExceptionCount)
                                return;

                            break;
                        }

                        using (var creatorScope = context.BeginScope(creatorScopeKind))
                        {
                            mainProcess = table.PartitionedMainProcessCreator.Invoke(Configuration.ConnectionStringKey, table, partitionIndex);
                        }

                        mainProcess.Name += "-#" + (partitionIndex + 1).ToString("D", CultureInfo.InvariantCulture);
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
                    using (var scope = context.BeginScope(Configuration.FinalizerTransactionScopeKind))
                    {
                        if (Configuration.BeforeFinalizersJobCreator != null)
                        {
                            context.Log(LogSeverity.Information, this, "starting global finalizers #" + retryCounter.ToString("D", CultureInfo.InvariantCulture));
                            List<IJob> beforeFinalizerJobs;

                            using (var creatorScope = context.BeginScope(TransactionScopeKind.Suppress))
                            {
                                beforeFinalizerJobs = Configuration.BeforeFinalizersJobCreator.Invoke(Configuration.ConnectionStringKey, Configuration);
                            }

                            var process = new JobProcess(context, "BeforeFinalizer");
                            var index = 0;
                            foreach (var job in beforeFinalizerJobs)
                            {
                                job.Name = beforeFinalizerJobs.Count == 1
                                    ? "BeforeFinalizer-" + job.Name
                                    : "BeforeFinalizer-" + index.ToString("D", CultureInfo.InvariantCulture) + "-" + job.Name;
                                process.AddJob(job);

                                index++;
                            }

                            process.EvaluateWithoutResult(this);
                        }

                        context.Log(LogSeverity.Information, this, "starting table-specific finalizers #" + retryCounter.ToString("D", CultureInfo.InvariantCulture));
                        foreach (var table in Configuration.Tables)
                        {
                            var creatorScopeKind = table.SuppressTransactionScopeForCreators
                                ? TransactionScopeKind.Suppress
                                : TransactionScopeKind.None;

                            List<IJob> finalizerJobs;
                            using (var creatorScope = context.BeginScope(creatorScopeKind))
                            {
                                finalizerJobs = table.FinalizerJobsCreator.Invoke(Configuration.ConnectionStringKey, table);
                            }

                            var process = new JobProcess(context, "Finalizer-" + Helpers.UnEscapeTableName(table.TableName));
                            var index = 0;
                            foreach (var job in finalizerJobs)
                            {
                                job.Name = finalizerJobs.Count == 1
                                    ? "Finalizer-" + Helpers.UnEscapeTableName(table.TableName) + "-" + job.Name
                                    : "Finalizer-" + Helpers.UnEscapeTableName(table.TableName) + "-" + index.ToString("D", CultureInfo.InvariantCulture) + "-" + job.Name;
                                process.AddJob(job);

                                index++;
                            }

                            process.EvaluateWithoutResult(this);
                        }

                        if (Configuration.AfterFinalizersJobCreator != null)
                        {
                            List<IJob> afterFinalizerJobs;

                            using (var creatorScope = context.BeginScope(TransactionScopeKind.Suppress))
                            {
                                afterFinalizerJobs = Configuration.AfterFinalizersJobCreator.Invoke(Configuration.ConnectionStringKey, Configuration);
                            }

                            var process = new JobProcess(context, "AfterFinalizer");
                            var index = 0;
                            foreach (var job in afterFinalizerJobs)
                            {
                                job.Name = afterFinalizerJobs.Count == 1
                                    ? "AfterFinalizer-" + job.Name
                                    : "AfterFinalizer-" + index.ToString("D", CultureInfo.InvariantCulture) + "-" + job.Name;
                                process.AddJob(job);

                                index++;
                            }

                            process.EvaluateWithoutResult(this);
                        }

                        var currentExceptionCount = context.GetExceptions().Count;
                        if (currentExceptionCount == initialExceptionCount)
                        {
                            scope?.Complete();

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
        }

        public static IJob DeleteTargetTableFinalizer(string connectionStringKey, DwhStrategyTableConfigurationBase tableConfiguration)
        {
            return new DeleteTableJob
            {
                Name = "DeleteContentFromTargetTable",
                ConnectionStringKey = connectionStringKey,
                TableName = tableConfiguration.TableName,
            };
        }

        public static IJob CopyTableFinalizer(string connectionStringKey, DwhStrategyTableConfigurationBase tableConfiguration, int commandTimeout, bool copyIdentityColumns = false)
        {
            return new CopyTableIntoExistingTableJob
            {
                Name = "CopyTempToTargetTable",
                ConnectionStringKey = connectionStringKey,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = tableConfiguration.TempTableName,
                    TargetTableName = tableConfiguration.TableName,
                    ColumnConfiguration = tableConfiguration.Columns?.Select(x => new ColumnCopyConfiguration(x)).ToList(),
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
                    ColumnConfiguration = table.Columns?.Select(x => new ColumnCopyConfiguration(x)).ToList(),
                });

                if (table.AdditionalTables != null)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        config.Add(new TableCopyConfiguration()
                        {
                            SourceTableName = additionalTable.TableName,
                            TargetTableName = additionalTable.TempTableName,
                            ColumnConfiguration = additionalTable.Columns?.Select(x => new ColumnCopyConfiguration(x)).ToList(),
                        });
                    }
                }
            }

            var process = new JobProcess(context, "RecreateTempTables");
            process.AddJob(new CopyTableStructureJob
            {
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

            var process = new JobProcess(context, "DropTempTablesProcess");
            process.AddJob(new DropTablesJob()
            {
                Name = "DropTempTables",
                ConnectionStringKey = Configuration.ConnectionStringKey,
                TableNames = tempTableNames.Concat(additionalTempTableNames).ToArray(),
            });

            process.EvaluateWithoutResult(this);
        }
    }
}