namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    internal class ResilientTableFinalizerManager
    {
        public void Execute(IEtlContext context, ResilientSqlScope scope, ConnectionStringWithProvider connectionString)
        {
            var tempTablesWithData = 0;
            var tempTablesWithoutData = 0;
            foreach (var table in scope.Configuration.Tables)
            {
                var rowCount = CountRowsIn(scope, context, connectionString, table.TempTableName);

                if (table.AdditionalTables?.Count > 0)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        rowCount += CountRowsIn(scope, context, connectionString, additionalTable.TempTableName);
                    }
                }

                if (rowCount > 0)
                {
                    tempTablesWithData++;

                    context.Log(LogSeverity.Information, scope, "creating finalizers for {TableName}",
                        connectionString.Unescape(table.TableName));

                    var creatorScopeKind = table.SuppressTransactionScopeForCreators
                        ? TransactionScopeKind.Suppress
                        : TransactionScopeKind.None;

                    IExecutable[] finalizers;
                    using (var creatorScope = context.BeginScope(scope, null, creatorScopeKind, LogSeverity.Information))
                    {
                        finalizers = table.FinalizerCreator
                            .Invoke(table)
                            .Where(x => x != null)
                            .ToArray();
                    }

                    foreach (var finalizer in finalizers)
                    {
                        var preExceptionCount = context.GetExceptions().Count;
                        finalizer.Execute(scope);
                        if (context.GetExceptions().Count > preExceptionCount)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    tempTablesWithoutData++;
                    context.Log(LogSeverity.Debug, scope, "no data found for {TableName}, skipping finalizers", connectionString.Unescape(table.TableName));
                }
            }

            context.Log(LogSeverity.Information, scope, "{TableCount} temp table contains data", tempTablesWithData);
            context.Log(LogSeverity.Information, scope, "{TableCount} temp table is empty", tempTablesWithoutData);
        }

        private static int CountRowsIn(ResilientSqlScope scope, IEtlContext context, ConnectionStringWithProvider connectionString, string tempTableName)
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

            context.Log(count > 0 ? LogSeverity.Information : LogSeverity.Debug, scope, "{TempRowCount} rows found in {TableName}", count, connectionString.Unescape(tempTableName));

            return count;
        }
    }
}