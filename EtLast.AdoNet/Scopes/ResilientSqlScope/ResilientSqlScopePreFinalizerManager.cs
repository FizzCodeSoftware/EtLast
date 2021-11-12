namespace FizzCode.EtLast.AdoNet
{
    using System.Linq;

    internal sealed class ResilientSqlScopePreFinalizerManager : IProcess
    {
        public ProcessInvocationInfo InvocationInfo { get; set; }

        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public string Name { get; } = "PreFinalizerManager";
        public ITopic Topic => _scope.Topic;
        public ProcessKind Kind => ProcessKind.scope;

        internal ResilientSqlScopePreFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
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
            }

            if (finalizers?.Length > 0)
            {
                Context.Log(LogSeverity.Debug, this, "created {PreFinalizerCount} pre-finalizer(s)",
                    finalizers?.Length ?? 0);

                foreach (var finalizer in finalizers)
                {
                    var preExceptionCount = Context.ExceptionCount;

                    Context.Log(LogSeverity.Information, this, "starting pre-finalizer: {Process}",
                        finalizer.Name);

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