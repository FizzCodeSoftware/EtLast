namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal class DwhStrategyTableFinalizerManager : ICaller
    {
        public ICaller Caller { get; private set; }
        public string Name => "TableFinalizerManager";

        public void Execute(IEtlContext context, DwhStrategy strategy)
        {
            Caller = strategy;

            context.Log(LogSeverity.Information, this, "started");
            var tempTablesWithData = 0;
            var tempTablesWithoutData = 0;
            foreach (var table in strategy.Configuration.Tables)
            {
                var rowCount = new CustomSqlAdoNetDbReaderProcess(context, "RowCountReader")
                {
                    ConnectionStringKey = strategy.Configuration.ConnectionStringKey,
                    Sql = "SELECT COUNT(*) cnt FROM " + table.TempTableName,
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("cnt", new IntConverter(), NullSourceHandler.SetSpecialValue)
                        {
                            SpecialValueIfSourceIsNull = 0,
                        }
                    }
                }.Evaluate().ToList().FirstOrDefault()?.GetAs<int>("cnt") ?? 0;

                // todo: support AdditionalTables here !
                if (table.AdditionalTables?.Count > 0)
                {
                    throw new NotImplementedException();
                }

                if (rowCount > 0)
                {
                    tempTablesWithData++;

                    context.Log(LogSeverity.Information, this, "{TempRowCount} rows found in {TableName}, creating finalizers for table",
                        rowCount, Helpers.UnEscapeTableName(table.TempTableName));

                    var creatorScopeKind = table.SuppressTransactionScopeForCreators
                    ? TransactionScopeKind.Suppress
                    : TransactionScopeKind.None;

                    List<IJob> finalizerJobs;
                    using (var creatorScope = context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
                    {
                        finalizerJobs = table.FinalizerJobsCreator.Invoke(strategy.Configuration.ConnectionStringKey, table);
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
                    context.Log(LogSeverity.Debug, this, "no data found in {TableName}, skipping finalizers", Helpers.UnEscapeTableName(table.TempTableName));
                }
            }

            context.Log(LogSeverity.Information, this, "{TableCount} temp table contains data", tempTablesWithData);
            context.Log(LogSeverity.Information, this, "{TableCount} temp table contains no data", tempTablesWithoutData);
        }
    }
}