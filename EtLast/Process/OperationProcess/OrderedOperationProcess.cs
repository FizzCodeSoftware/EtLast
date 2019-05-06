namespace FizzCode.EtLast
{
    public class OrderedOperationProcess : AbstractOperationProcess
    {
        public BasicOperationProcessConfiguration Configuration { get; set; } = new BasicOperationProcessConfiguration();

        protected override BasicOperationProcessConfiguration BasicConfiguration => Configuration;
        protected override bool KeepOrder => true;

        public OrderedOperationProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        protected override void Validate()
        {
            if (Configuration == null) throw new ProcessParameterNullException(this, nameof(Configuration));
            if (Configuration.RowQueueType == null) throw new ProcessParameterNullException(this, nameof(Configuration.RowQueueType));
            if (InputProcess == null) throw new ProcessParameterNullException(this, nameof(InputProcess));
        }

        protected override void CreateWorkers()
        {
            CreateWorkerThreads(1, typeof(DefaultInProcessWorker));
            if (Context.CancellationTokenSource.IsCancellationRequested) return;
        }
    }
}