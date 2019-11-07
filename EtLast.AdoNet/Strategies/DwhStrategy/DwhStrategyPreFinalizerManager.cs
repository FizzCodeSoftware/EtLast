namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    internal class DwhStrategyPreFinalizerManager
    {
        public void Execute(IEtlContext context, DwhStrategy strategy)
        {
            List<IExecutable> finalizers;

            context.Log(LogSeverity.Information, strategy, "started");
            using (var creatorScope = context.BeginScope(strategy, null, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                finalizers = strategy.Configuration.BeforeFinalizersJobCreator.Invoke(strategy.Configuration.ConnectionStringKey, strategy.Configuration);
                context.Log(LogSeverity.Information, strategy, "created {PreFinalizerCount} pre-finalizers", finalizers?.Count ?? 0);
            }

            if (finalizers?.Count > 0)
            {
                context.Log(LogSeverity.Information, strategy, "starting pre-finalizers");

                foreach (var finalizer in finalizers)
                {
                    var preExceptionCount = context.GetExceptions().Count;
                    finalizer.Execute(strategy);
                    if (context.GetExceptions().Count > preExceptionCount)
                    {
                        break;
                    }
                }
            }
        }
    }
}