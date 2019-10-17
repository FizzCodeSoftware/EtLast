namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    internal class DwhStrategyPostFinalizerManager : ICaller
    {
        public ICaller Caller { get; private set; }
        public string Name => "PostFinalizerManager";

        public void Execute(IEtlContext context, DwhStrategy strategy)
        {
            Caller = strategy;

            List<IJob> jobs;

            context.Log(LogSeverity.Information, this, "started");
            using (var creatorScope = context.BeginScope(this, null, null, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                jobs = strategy.Configuration.AfterFinalizersJobCreator.Invoke(strategy.Configuration.ConnectionStringKey, strategy.Configuration);
                context.Log(LogSeverity.Information, this, "created {PostFinalizerCount} post-finalizers", jobs?.Count ?? 0);
            }

            if (jobs?.Count > 0)
            {
                context.Log(LogSeverity.Information, this, "starting post-finalizers");

                var process = new JobHostProcess(context, "PostFinalizerHost");
                foreach (var job in jobs)
                {
                    process.AddJob(job);
                }

                process.EvaluateWithoutResult(this);
            }
        }
    }
}