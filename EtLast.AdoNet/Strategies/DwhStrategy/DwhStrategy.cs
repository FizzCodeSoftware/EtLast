namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    public class DwhStrategy : IEtlStrategy
    {
        public DwhStrategyConfiguration Configuration { get; }

        public DwhStrategy(DwhStrategyConfiguration configuration)
        {
            Configuration = configuration;
        }

        public void Execute(IEtlContext context)
        {
            var maxRetryCount = Configuration.FinalizerRetryCount;
            if (Configuration.FinalizerTransactionScopeKind != TransactionScopeKind.RequiresNew && maxRetryCount > 0)
            {
                throw new InvalidStrategyParameterException(this, nameof(Configuration.FinalizerRetryCount), null, "retrying finalizers can be possible only if the " + nameof(Configuration.FinalizerTransactionScopeKind) + " is set to " + nameof(TransactionScopeKind.RequiresNew));
            }

            var initialExceptionCount = context.GetExceptions().Count;
            var success = false;

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

                        var processSupportsPartitions = false;
                        using (var creatorScope = context.BeginScope(creatorScopeKind))
                        {
                            mainProcess = table.MainProcessCreator.Invoke(Configuration.ConnectionStringKey, table, partitionIndex, out processSupportsPartitions);
                        }

                        if (processSupportsPartitions)
                        {
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
                        else
                        {
                            mainProcess.EvaluateWithoutResult();

                            if (context.GetExceptions().Count > initialExceptionCount)
                                return;

                            break;
                        }
                    }
                }

                for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
                {
                    using (var scope = context.BeginScope(Configuration.FinalizerTransactionScopeKind))
                    {
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

                            var process = new JobProcess(context, "Finalizer-" + table.TableName.Replace("[", "").Replace("]", ""));
                            var index = 0;
                            foreach (var job in finalizerJobs)
                            {
                                job.Name = finalizerJobs.Count == 1
                                    ? "Finalizer-" + table.TableName.Replace("[", "").Replace("]", "") + "-" + job.Name
                                    : "Finalizer-" + table.TableName.Replace("[", "").Replace("]", "") + "-" + index.ToString("D", CultureInfo.InvariantCulture) + "-" + job.Name;
                                process.AddJob(job);

                                index++;
                            }

                            process.EvaluateWithoutResult();
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
                if (success || Configuration.AlwaysDropTempTables)
                {
                    DropTempTables(context);
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

            var process = new JobProcess(context, "DropCreateTempTables");
            process.AddJob(new CopyTableStructureJob
            {
                ConnectionStringKey = Configuration.ConnectionStringKey,
                SuppressExistingTransactionScope = true,
                Configuration = config,
            });

            process.EvaluateWithoutResult();
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

            process.EvaluateWithoutResult();
        }
    }
}