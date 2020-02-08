namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;
    using System.Linq;

    internal class ResilientSqlScopePostFinalizerManager : IProcess
    {
        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public int UID { get; }
        public string Name { get; } = "PostFinalizerManager";
        public string Topic => _scope.Topic;
        public IProcess Caller => _scope;
        public Stopwatch LastInvocation { get; private set; }
        public StatCounterCollection CounterCollection { get; }

        public ResilientSqlScopePostFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
            CounterCollection = new StatCounterCollection(scope.Context.CounterCollection);
            UID = Context.GetProcessUid(this);
        }

        public void Execute()
        {
            LastInvocation = Stopwatch.StartNew();

            IExecutable[] finalizers;

            Context.Log(LogSeverity.Information, this, "started");
            using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                finalizers = _scope.Configuration.PostFinalizerCreator.Invoke(_scope)
                    ?.Where(x => x != null)
                    .ToArray();

                Context.Log(LogSeverity.Information, this, "created {PostFinalizerCount} post-finalizers", finalizers?.Length ?? 0);
            }

            if (finalizers?.Length > 0)
            {
                Context.Log(LogSeverity.Information, this, "starting post-finalizers");

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
        }

        public void Validate()
        {
        }
    }
}