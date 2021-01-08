namespace FizzCode.EtLast.AdoNet
{
    using System.Linq;
    using FizzCode.EtLast;

    internal class ResilientSqlScopeInitializerManager : IProcess
    {
        public ProcessInvocationInfo InvocationInfo { get; set; }

        private readonly ResilientSqlScope _scope;
        public IEtlContext Context => _scope.Context;
        public string Name { get; } = "InitializerManager";
        public ITopic Topic => _scope.Topic;
        public ProcessKind Kind => ProcessKind.scope;

        internal ResilientSqlScopeInitializerManager(ResilientSqlScope scope)
        {
            _scope = scope;
        }

        public void Execute()
        {
            Context.RegisterProcessInvocationStart(this, _scope);

            IExecutable[] initializers;

            using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                initializers = _scope.Configuration.InitializerCreator.Invoke(_scope, this)
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

            Context.RegisterProcessInvocationEnd(this);
        }
    }
}