namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;
    using System.Linq;

    internal class ResilientTableFinalizerManager : IProcess
    {
        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public int InvocationUID { get; set; }
        public int InstanceUID { get; set; }
        public int InvocationCounter { get; set; }
        public string Name { get; } = "TableFinalizerManager";
        public string Topic => _scope.Topic;
        public IProcess Caller => _scope;
        public Stopwatch LastInvocation { get; private set; }
        public StatCounterCollection CounterCollection { get; }

        public ResilientTableFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
            CounterCollection = new StatCounterCollection(scope.Context.CounterCollection);
        }

        public void Execute()
        {
            Context.GetProcessUid(this);

            LastInvocation = Stopwatch.StartNew();

            Context.Log(LogSeverity.Information, this, "started");

            var tempTablesWithData = 0;
            var tempTablesWithoutData = 0;
            foreach (var table in _scope.Configuration.Tables)
            {
                var recordCount = CountTempRecordsIn(table);

                if (table.AdditionalTables?.Count > 0)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        recordCount += CountTempRecordsIn(additionalTable);
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
                    using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
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

        private int CountTempRecordsIn(ResilientTableBase table)
        {
            var count = new GetTableRecordCountProcess(Context, "TempRecordCountReader", table.Topic)
            {
                ConnectionString = _scope.Configuration.ConnectionString,
                TableName = _scope.Configuration.ConnectionString.Escape(table.TempTableName),
            }.Execute();

            if (count > 0)
            {
                Context.Log(LogSeverity.Information, this, "{TempRecordCount} records found in {ConnectionStringName}/{TableName}",
                      count, _scope.Configuration.ConnectionString.Name, _scope.Configuration.ConnectionString.Unescape(table.TempTableName));
            }

            return count;
        }
    }
}