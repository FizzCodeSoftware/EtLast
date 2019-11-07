namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    internal class DwhStrategyTableFinalizerManager
    {
        public void Execute(IEtlContext context, DwhStrategy strategy, ConnectionStringWithProvider connectionString)
        {
            var tempTablesWithData = 0;
            var tempTablesWithoutData = 0;
            foreach (var table in strategy.Configuration.Tables)
            {
                var rowCount = CountRowsIn(strategy, context, connectionString, table.TempTableName);

                if (table.AdditionalTables?.Count > 0)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        rowCount += CountRowsIn(strategy, context, connectionString, additionalTable.TempTableName);
                    }
                }

                if (rowCount > 0)
                {
                    tempTablesWithData++;

                    context.Log(LogSeverity.Information, strategy, "creating finalizers for {TableName}",
                        connectionString.Unescape(table.TableName));

                    var creatorScopeKind = table.SuppressTransactionScopeForCreators
                        ? TransactionScopeKind.Suppress
                        : TransactionScopeKind.None;

                    List<IExecutable> finalizers;
                    using (var creatorScope = context.BeginScope(strategy, null, creatorScopeKind, LogSeverity.Information))
                    {
                        finalizers = table.FinalizerJobsCreator.Invoke(table);
                    }

                    foreach (var finalizer in finalizers)
                    {
                        var preExceptionCount = context.GetExceptions().Count;
                        finalizer.Execute(strategy);
                        if (context.GetExceptions().Count > preExceptionCount)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    tempTablesWithoutData++;
                    context.Log(LogSeverity.Debug, strategy, "no data found for {TableName}, skipping finalizers", connectionString.Unescape(table.TableName));
                }
            }

            context.Log(LogSeverity.Information, strategy, "{TableCount} temp table contains data", tempTablesWithData);
            context.Log(LogSeverity.Information, strategy, "{TableCount} temp table is empty", tempTablesWithoutData);
        }

        private static int CountRowsIn(DwhStrategy strategy, IEtlContext context, ConnectionStringWithProvider connectionString, string tempTableName)
        {
            var count = new CustomSqlAdoNetDbReaderProcess(context, "TempRowCountReader:" + connectionString.Unescape(tempTableName))
            {
                ConnectionStringKey = connectionString.Name,
                Sql = "select count(*) as cnt from " + tempTableName,
                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                {
                    new ReaderColumnConfiguration("cnt", new IntConverter(), NullSourceHandler.SetSpecialValue)
                    {
                        SpecialValueIfSourceIsNull = 0,
                    }
                }
            }.Evaluate().ToList().FirstOrDefault()?.GetAs<int>("cnt") ?? 0;

            context.Log(count > 0 ? LogSeverity.Information : LogSeverity.Debug, strategy, "{TempRowCount} rows found in {TableName}", count, connectionString.Unescape(tempTableName));

            return count;
        }
    }
}