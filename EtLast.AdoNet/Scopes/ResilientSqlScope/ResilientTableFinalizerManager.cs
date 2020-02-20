namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Diagnostics;
    using System.Linq;

    internal class ResilientTableFinalizerManager : IProcess
    {
        public int InvocationUID { get; set; }
        public int InstanceUID { get; set; }
        public int InvocationCounter { get; set; }
        public IProcess Caller { get; set; }
        public Stopwatch LastInvocationStarted { get; set; }
        public DateTimeOffset? LastInvocationFinished { get; set; }

        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public string Name { get; } = "TableFinalizerManager";
        public ITopic Topic => _scope.Topic;
        public ProcessKind Kind => ProcessKind.unknown;
        public StatCounterCollection CounterCollection { get; }

        public ResilientTableFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
            CounterCollection = new StatCounterCollection(scope.Context.CounterCollection);
        }

        public void Execute()
        {
            Context.RegisterProcessInvocationStart(this, _scope);

            var recordCounts = new int[_scope.Configuration.Tables.Count];
            for (var i = 0; i < _scope.Configuration.Tables.Count; i++)
            {
                var table = _scope.Configuration.Tables[i];

                var recordCount = CountTempRecordsIn(table);
                if (table.AdditionalTables?.Count > 0)
                {
                    foreach (var additionalTable in table.AdditionalTables.Values)
                    {
                        recordCount += CountTempRecordsIn(additionalTable);
                    }
                }

                recordCounts[i] = recordCount;
                if (recordCount == 0)
                {
                    Context.Log(LogSeverity.Debug, this, "no data found for {TableName}, skipping finalizers", _scope.Configuration.ConnectionString.Unescape(table.TableName));
                }
            }

            Context.Log(LogSeverity.Information, this, "{TableCount} temp table contains data", recordCounts.Count(x => x > 0));
            Context.Log(LogSeverity.Information, this, "{TableCount} temp table is empty", recordCounts.Count(x => x == 0));

            for (var i = 0; i < _scope.Configuration.Tables.Count; i++)
            {
                var table = _scope.Configuration.Tables[i];
                var recordCount = recordCounts[i];
                if (recordCount == 0)
                    continue;

                Context.Log(LogSeverity.Information, this, "finalizing table {TableName}",
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

            Context.RegisterProcessInvocationEnd(this);
        }

        private int CountTempRecordsIn(ResilientTableBase table)
        {
            var count = new GetTableRecordCountProcess(table.Topic, "TempRecordCountReader")
            {
                ConnectionString = _scope.Configuration.ConnectionString,
                TableName = _scope.Configuration.ConnectionString.Escape(table.TempTableName),
            }.Execute(this);

            if (count > 0)
            {
                Context.Log(LogSeverity.Debug, this, "{TempRecordCount} records found in {ConnectionStringName}/{TableName}",
                      count, _scope.Configuration.ConnectionString.Name, _scope.Configuration.ConnectionString.Unescape(table.TempTableName));
            }

            return count;
        }
    }
}