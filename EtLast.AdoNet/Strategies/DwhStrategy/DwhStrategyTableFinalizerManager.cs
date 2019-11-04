namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Linq;

    internal class DwhStrategyTableFinalizerManager : IExecutionBlock
    {
        public IExecutionBlock Caller { get; private set; }
        public string Name => "TableFinalizerManager";

        public void Execute(IEtlContext context, DwhStrategy strategy)
        {
            Caller = strategy;

            context.Log(LogSeverity.Information, this, "started");
            var tempTablesWithData = 0;
            var tempTablesWithoutData = 0;
            foreach (var table in strategy.Configuration.Tables)
            {
                var rowCount = CountRowsIn(context, strategy.Configuration.ConnectionStringKey, table.TempTableName);

                if (table.AdditionalTables?.Count > 0)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        rowCount += CountRowsIn(context, strategy.Configuration.ConnectionStringKey, additionalTable.TempTableName);
                    }
                }

                if (rowCount > 0)
                {
                    tempTablesWithData++;

                    context.Log(LogSeverity.Information, this, "creating finalizers for {TableName}",
                        Helpers.UnEscapeTableName(table.TableName));

                    var creatorScopeKind = table.SuppressTransactionScopeForCreators
                        ? TransactionScopeKind.Suppress
                        : TransactionScopeKind.None;

                    List<IJob> finalizerJobs;
                    using (var creatorScope = context.BeginScope(this, null, null, creatorScopeKind, LogSeverity.Information))
                    {
                        finalizerJobs = table.FinalizerJobsCreator.Invoke(table);
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
                    tempTablesWithoutData++;
                    context.Log(LogSeverity.Debug, this, "no data found for {TableName}, skipping finalizers", Helpers.UnEscapeTableName(table.TableName));
                }
            }

            context.Log(LogSeverity.Information, this, "{TableCount} temp table contains data", tempTablesWithData);
            context.Log(LogSeverity.Information, this, "{TableCount} temp table contains no data", tempTablesWithoutData);
        }

        private int CountRowsIn(IEtlContext context, string connectionStringKey, string tempTableName)
        {
            var count = new CustomSqlAdoNetDbReaderProcess(context, "TempRowCountReader:" + tempTableName)
            {
                ConnectionStringKey = connectionStringKey,
                Sql = "select count(*) as cnt from " + tempTableName,
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                {
                    new ReaderColumnConfiguration("cnt", new IntConverter(), NullSourceHandler.SetSpecialValue)
                    {
                        SpecialValueIfSourceIsNull = 0,
                    }
                }
            }.Evaluate().ToList().FirstOrDefault()?.GetAs<int>("cnt") ?? 0;

            context.Log(count > 0 ? LogSeverity.Information : LogSeverity.Debug, this, "{TempRowCount} rows found in {TableName}", count, tempTableName);

            return count;
        }
    }
}