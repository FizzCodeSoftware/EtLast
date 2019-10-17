namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    internal class DwhStrategyPreFinalizerManager : ICaller
    {
        public ICaller Caller { get; private set; }
        public string Name => "PreFinalizerManager";

        public void Execute(IEtlContext context, DwhStrategy strategy)
        {
            Caller = strategy;

            List<IJob> jobs;

            context.Log(LogSeverity.Information, this, "started");
            using (var creatorScope = context.BeginScope(this, null, null, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                jobs = strategy.Configuration.BeforeFinalizersJobCreator.Invoke(strategy.Configuration.ConnectionStringKey, strategy.Configuration);
                context.Log(LogSeverity.Information, this, "created {PreFinalizerCount} pre-finalizers", jobs?.Count ?? 0);
            }

            if (jobs?.Count > 0)
            {
                context.Log(LogSeverity.Information, this, "starting pre-finalizers");

                var process = new JobHostProcess(context, "PreFinalizerHost");
                foreach (var job in jobs)
                {
                    process.AddJob(job);
                }

                process.EvaluateWithoutResult(this);
            }
        }
    }
}