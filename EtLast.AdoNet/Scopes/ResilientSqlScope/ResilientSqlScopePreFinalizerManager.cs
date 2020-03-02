namespace FizzCode.EtLast.AdoNet
{
    using System.Linq;

    internal class ResilientSqlScopePreFinalizerManager : IProcess
    {
        public ProcessInvocationInfo InvocationInfo { get; set; }

        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public string Name { get; } = "PreFinalizerManager";
        public ITopic Topic => _scope.Topic;
        public ProcessKind Kind => ProcessKind.scope;
        public StatCounterCollection CounterCollection { get; }

        public ResilientSqlScopePreFinalizerManager(ResilientSqlScope scope)
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
                finalizers = _scope.Configuration.PreFinalizerCreator.Invoke(_scope, this)
                    ?.Where(x => x != null)
                    .ToArray();

                Context.Log(LogSeverity.Information, this, "created {PreFinalizerCount} pre-finalizers", finalizers?.Length ?? 0);
            }

            if (finalizers?.Length > 0)
            {
                Context.Log(LogSeverity.Information, this, "starting pre-finalizers");

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