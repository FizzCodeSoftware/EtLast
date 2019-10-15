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

        public DwhStrategyConfiguration Configuration { get; set; }

        public void Execute(ICaller caller, IEtlContext context)
        {
            Caller = caller;
            context.Log(LogSeverity.Information, this, "data warehouse strategy started");
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

                foreach (var table in Configuration.Tables)
                {
                    if (table.MainProcessCreator != null)
                        context.Log(LogSeverity.Information, this, "processing table {TableName}", Helpers.UnEscapeTableName(table.TableName));

                    for (var partitionIndex = 0; ; partitionIndex++)
                    {
                        if (table.PartitionedMainProcessCreator != null)
                            context.Log(LogSeverity.Information, this, "processing table {TableName} (partition #{PartitionIndex})", Helpers.UnEscapeTableName(table.TableName), partitionIndex);

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
                    using (var scope = context.BeginScope(Configuration.FinalizerTransactionScopeKind))
                    {
                        if (Configuration.BeforeFinalizersJobCreator != null)
                        {
                            List<IJob> beforeFinalizerJobs;

                            using (var creatorScope = context.BeginScope(TransactionScopeKind.Suppress))
                            {
                                beforeFinalizerJobs = Configuration.BeforeFinalizersJobCreator.Invoke(Configuration.ConnectionStringKey, Configuration);
                            }

                            if (beforeFinalizerJobs?.Count > 0)
                            {
                                context.Log(LogSeverity.Information, this, "starting 'before' finalizers #" + retryCounter.ToString("D", CultureInfo.InvariantCulture));

                                var process = new JobHostProcess(context, "BeforeFinalizers");
                                foreach (var job in beforeFinalizerJobs)
                                {
                                    process.AddJob(job);
                                }

                                process.EvaluateWithoutResult(this);
                            }
                        }

                        context.Log(LogSeverity.Information, this, "starting table-specific finalizers #" + retryCounter.ToString("D", CultureInfo.InvariantCulture));
                        foreach (var table in Configuration.Tables)
                        {
                            var rowCount = new CustomSqlAdoNetDbReaderProcess(context, "RowCountReader")
                            {
                                ConnectionStringKey = Configuration.ConnectionStringKey,
                                Sql = "SELECT COUNT(*) cnt FROM " + table.TempTableName,
                                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                                {
                                    new ReaderColumnConfiguration("cnt", new IntConverter(), NullSourceHandler.SetSpecialValue)
                                    {
                                        SpecialValueIfSourceIsNull = 0,
                                    }
                                }
                            }.Evaluate().ToList().FirstOrDefault()?.GetAs<int>("cnt") ?? 0;

                            if (rowCount > 0)
                            {
                                context.Log(LogSeverity.Information, this, "{TempRowCount} rows found in {TableName}, starting finalizers", rowCount, Helpers.UnEscapeTableName(table.TempTableName));

                                var creatorScopeKind = table.SuppressTransactionScopeForCreators
                                ? TransactionScopeKind.Suppress
                                : TransactionScopeKind.None;

                                List<IJob> finalizerJobs;
                                using (var creatorScope = context.BeginScope(creatorScopeKind))
                                {
                                    finalizerJobs = table.FinalizerJobsCreator.Invoke(Configuration.ConnectionStringKey, table);
                                }

                                var process = new JobHostProcess(context, "Finalize:" + Helpers.UnEscapeTableName(table.TableName));
                                foreach (var job in finalizerJobs)
                                {
                                    process.AddJob(job);
                                }

                                process.EvaluateWithoutResult(this);
                            }
                            else
                            {
                                context.Log(LogSeverity.Debug, this, "{TempRowCount} rows found in {TableName}, skipping finalizers", rowCount, Helpers.UnEscapeTableName(table.TempTableName));
                            }
                        }

                        if (Configuration.AfterFinalizersJobCreator != null)
                        {
                            List<IJob> afterFinalizerJobs;

                            using (var creatorScope = context.BeginScope(TransactionScopeKind.Suppress))
                            {
                                afterFinalizerJobs = Configuration.AfterFinalizersJobCreator.Invoke(Configuration.ConnectionStringKey, Configuration);
                            }

                            if (afterFinalizerJobs?.Count > 0)
                            {
                                context.Log(LogSeverity.Information, this, "starting 'after' finalizers #" + retryCounter.ToString("D", CultureInfo.InvariantCulture));

                                var process = new JobHostProcess(context, "AfterFinalizers");
                                foreach (var job in afterFinalizerJobs)
                                {
                                    process.AddJob(job);
                                }

                                process.EvaluateWithoutResult(this);
                            }
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

            context.Log(LogSeverity.Information, this, success ? "finished in {Elapsed}" : "failed after {Elapsed}", startedOn.Elapsed);
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

            var process = new JobHostProcess(context, "RecreateTempTables");
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

            var process = new JobHostProcess(context, "DropTempTablesProcess");
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