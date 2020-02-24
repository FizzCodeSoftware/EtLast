namespace FizzCode.EtLast.AdoNet
{
    using System.Linq;

    internal class ResilientSqlScopePostFinalizerManager : IProcess
    {
        public ProcessInvocationInfo InvocationInfo { get; set; }

        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public string Name { get; } = "PostFinalizerManager";
        public ITopic Topic => _scope.Topic;
        public ProcessKind Kind => ProcessKind.unknown;
        public StatCounterCollection CounterCollection { get; }

        public ResilientSqlScopePostFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
            CounterCollection = new StatCounterCollection(scope.Context.CounterCollection);
        }

        public void Execute()
        {
            Context.RegisterProcessInvocationStart(this, _scope);

            IExecutable[] finalizers;

            using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                finalizers = _scope.Configuration.PostFinalizerCreator.Invoke(_scope, this)
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

            Context.RegisterProcessInvocationEnd(this);
        }
    }
}