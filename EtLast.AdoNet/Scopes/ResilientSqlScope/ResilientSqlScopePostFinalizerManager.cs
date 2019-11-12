namespace FizzCode.EtLast.AdoNet
{
    using System.Linq;

    internal class ResilientSqlScopePostFinalizerManager
    {
        public void Execute(IEtlContext context, ResilientSqlScope scope)
        {
            IExecutable[] finalizers;

            context.Log(LogSeverity.Information, scope, "started");
            using (var creatorScope = context.BeginScope(scope, null, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                finalizers = scope.Configuration.PostFinalizerCreator.Invoke(scope.Configuration.ConnectionStringKey, scope.Configuration)
                    ?.Where(x => x != null)
                    .ToArray();

                context.Log(LogSeverity.Information, scope, "created {PostFinalizerCount} post-finalizers", finalizers?.Length ?? 0);
            }

            if (finalizers?.Length > 0)
            {
                context.Log(LogSeverity.Information, scope, "starting post-finalizers");

                foreach (var finalizer in finalizers)
                {
                    var preExceptionCount = context.GetExceptions().Count;
                    finalizer.Execute(scope);
                    if (context.GetExceptions().Count > preExceptionCount)
                    {
                        break;
                    }
                }
            }
        }
    }
}