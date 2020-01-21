namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;
    using System.Linq;
    using FizzCode.EtLast;

    internal class ResilientSqlScopeInitializerManager : IProcess
    {
        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public int UID { get; }
        public string Name { get; } = "InitializerManager";
        public IProcess Caller => _scope;
        public Stopwatch LastInvocation { get; private set; }
        public ProcessTestDelegate If { get; set; }
        public StatCounterCollection CounterCollection { get; }

        public ResilientSqlScopeInitializerManager(ResilientSqlScope scope)
        {
            _scope = scope;
            CounterCollection = new StatCounterCollection(scope.Context.CounterCollection);
            UID = Context.GetProcessUid(this);
        }

        public void Execute()
        {
            LastInvocation = Stopwatch.StartNew();

            IExecutable[] initializers;

            Context.Log(LogSeverity.Information, this, "started");
            using (var creatorScope = Context.BeginScope(this, null, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                initializers = _scope.Configuration.InitializerCreator.Invoke(_scope)
                    ?.Where(x => x != null)
                    .ToArray();

                Context.Log(LogSeverity.Information, this, "created {InitializerCount} initializers", initializers?.Length ?? 0);
            }

            if (initializers?.Length > 0)
            {
                Context.Log(LogSeverity.Information, this, "starting initializers");

                foreach (var initializer in initializers)
                {
                    var preExceptionCount = Context.ExceptionCount;
                    initializer.Execute(this);
                    if (Context.ExceptionCount > preExceptionCount)
                    {
                        break;
                    }
                }
            }
        }

        public void Validate()
        {
        }
    }
}