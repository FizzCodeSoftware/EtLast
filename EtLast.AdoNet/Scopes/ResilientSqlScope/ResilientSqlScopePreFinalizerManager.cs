namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    internal class ResilientSqlScopePreFinalizerManager
    {
        public void Execute(IEtlContext context, ResilientSqlScope scope)
        {
            List<IExecutable> finalizers;

            context.Log(LogSeverity.Information, scope, "started");
            using (var creatorScope = context.BeginScope(scope, null, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                finalizers = scope.Configuration.PreFinalizerCreator.Invoke(scope.Configuration.ConnectionStringKey, scope.Configuration);
                context.Log(LogSeverity.Information, scope, "created {PreFinalizerCount} pre-finalizers", finalizers?.Count ?? 0);
            }

            if (finalizers?.Count > 0)
            {
                context.Log(LogSeverity.Information, scope, "starting pre-finalizers");

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