namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    internal class ResilientTableFinalizerManager : IProcess
    {
        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public string Name { get; } = "TableFinalizerManager";
        public IProcess Caller => _scope;
        public Stopwatch LastInvocation { get; private set; }
        public ProcessTestDelegate If { get; set; }
        public StatCounterCollection CounterCollection { get; }

        public ResilientTableFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
            CounterCollection = new StatCounterCollection(scope.Context.CounterCollection);
        }

        public void Execute(ConnectionStringWithProvider connectionString)
        {
            LastInvocation = Stopwatch.StartNew();

            Context.Log(LogSeverity.Information, this, "started");

            var tempTablesWithData = 0;
            var tempTablesWithoutData = 0;
            foreach (var table in _scope.Configuration.Tables)
            {
                var recordCount = CountRecordsIn(connectionString, table.TempTableName);

                if (table.AdditionalTables?.Count > 0)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        recordCount += CountRecordsIn(connectionString, additionalTable.TempTableName);
                    }
                }

                if (recordCount > 0)
                {
                    tempTablesWithData++;

                    Context.Log(LogSeverity.Information, this, "creating finalizers for {TableName}",
                        connectionString.Unescape(table.TableName));

                    var creatorScopeKind = table.SuppressTransactionScopeForCreators
                        ? TransactionScopeKind.Suppress
                        : TransactionScopeKind.None;

                    IExecutable[] finalizers;
                    using (var creatorScope = Context.BeginScope(this, null, creatorScopeKind, LogSeverity.Information))
                    {
                        finalizers = table.FinalizerCreator
                            .Invoke(table)
                            .Where(x => x != null)
                            .ToArray();
                    }

                    foreach (var finalizer in finalizers)
                    {
                        var preExceptionCount = Context.ExceptionCount;
                        finalizer.Execute(this);
                        if (Context.ExceptionCount > preExceptionCount)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    tempTablesWithoutData++;
                    Context.Log(LogSeverity.Debug, this, "no data found for {TableName}, skipping finalizers", connectionString.Unescape(table.TableName));
                }
            }

            Context.Log(LogSeverity.Information, this, "{TableCount} temp table contains data", tempTablesWithData);
            Context.Log(LogSeverity.Information, this, "{TableCount} temp table is empty", tempTablesWithoutData);
        }

        private int CountRecordsIn(ConnectionStringWithProvider connectionString, string tempTableName)
        {
            var count = new CustomSqlAdoNetDbReaderProcess(Context, "TempRecordCountReader:" + connectionString.Unescape(tempTableName))
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
            }.Evaluate(this).ToList().FirstOrDefault()?.GetAs<int>("cnt") ?? 0;

            Context.Log(count > 0 ? LogSeverity.Information : LogSeverity.Debug, this, "{TempRecordCount} records found in {TableName}", count, connectionString.Unescape(tempTableName));

            return count;
        }

        public void Validate()
        {
        }
    }
}