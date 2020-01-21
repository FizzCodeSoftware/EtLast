namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;
    using System.Linq;

    internal class ResilientTableFinalizerManager : IProcess
    {
        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public int UID { get; }
        public string Name { get; } = "TableFinalizerManager";
        public IProcess Caller => _scope;
        public Stopwatch LastInvocation { get; private set; }
        public ProcessTestDelegate If { get; set; }
        public StatCounterCollection CounterCollection { get; }

        public ResilientTableFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
            CounterCollection = new StatCounterCollection(scope.Context.CounterCollection);
            UID = Context.GetProcessUid(this);
        }

        public void Execute()
        {
            LastInvocation = Stopwatch.StartNew();

            Context.Log(LogSeverity.Information, this, "started");

            var tempTablesWithData = 0;
            var tempTablesWithoutData = 0;
            foreach (var table in _scope.Configuration.Tables)
            {
                var recordCount = CountRecordsIn(table.TempTableName);

                if (table.AdditionalTables?.Count > 0)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        recordCount += CountRecordsIn(additionalTable.TempTableName);
                    }
                }

                if (recordCount > 0)
                {
                    tempTablesWithData++;

                    Context.Log(LogSeverity.Information, this, "creating finalizers for {TableName}",
                        _scope.Configuration.ConnectionString.Unescape(table.TableName));

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
                    Context.Log(LogSeverity.Debug, this, "no data found for {TableName}, skipping finalizers", _scope.Configuration.ConnectionString.Unescape(table.TableName));
                }
            }

            Context.Log(LogSeverity.Information, this, "{TableCount} temp table contains data", tempTablesWithData);
            Context.Log(LogSeverity.Information, this, "{TableCount} temp table is empty", tempTablesWithoutData);
        }

        private int CountRecordsIn(string tempTableName)
        {
            var count = new GetTableRecordCountProcess(Context, "TempRecordCountReader:" + _scope.Configuration.ConnectionString.Unescape(tempTableName))
            {
                ConnectionString = _scope.Configuration.ConnectionString,
                TableName = tempTableName,
            }.Execute();

            if (count > 0)
            {
                Context.Log(LogSeverity.Information, this, "{TempRecordCount} records found in {ConnectionStringName}/{TableName}",
                      count, _scope.Configuration.ConnectionString.Name, _scope.Configuration.ConnectionString.Unescape(tempTableName));
            }

            return count;
        }

        public void Validate()
        {
        }
    }
}