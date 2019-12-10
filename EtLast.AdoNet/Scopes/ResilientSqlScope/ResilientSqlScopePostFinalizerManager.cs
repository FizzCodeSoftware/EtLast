namespace FizzCode.EtLast.AdoNet
{
    using System.Diagnostics;
    using System.Linq;

    internal class ResilientSqlScopePostFinalizerManager : IProcess
    {
        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public string Name { get; } = "PostFinalizerManager";
        public IProcess Caller => _scope;
        public Stopwatch LastInvocation { get; private set; }
        public ProcessTestDelegate If { get; set; }

        public ResilientSqlScopePostFinalizerManager(ResilientSqlScope scope)
        {
            _scope = scope;
        }

        public void Execute()
        {
            LastInvocation = Stopwatch.StartNew();

            IExecutable[] finalizers;

            Context.Log(LogSeverity.Information, this, "started");
            using (var creatorScope = Context.BeginScope(this, null, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                finalizers = _scope.Configuration.PostFinalizerCreator.Invoke(_scope.Configuration.ConnectionStringKey, _scope.Configuration)
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