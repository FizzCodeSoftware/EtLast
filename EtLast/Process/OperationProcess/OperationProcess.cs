namespace FizzCode.EtLast
{
    public class OperationProcess : AbstractOperationProcess
    {
        public OperationProcessConfiguration Configuration { get; set; } = new OperationProcessConfiguration();

        protected override BasicOperationProcessConfiguration BasicConfiguration => Configuration;
        protected override bool KeepOrder => Configuration.KeepOrder;

        public OperationProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        protected override void CreateWorkers()
        {
            CreateWorkerThreads(Configuration.WorkerCount, Configuration.WorkerType);
            if (Context.CancellationTokenSource.IsCancellationRequested)
                return;
        }

        protected override void Validate()
        {
            if (Configuration == null)
                throw new ProcessParameterNullException(this, nameof(Configuration));
            if (Configuration.WorkerCount <= 0)
                throw new InvalidProcessParameterException(this, nameof(Configuration.WorkerCount), Configuration.WorkerCount, "value must be greater than 0");
            if (Configuration.WorkerType == null)
                throw new ProcessParameterNullException(this, nameof(Configuration.WorkerType));
            if (Configuration.RowQueueType == null)
                throw new ProcessParameterNullException(this, nameof(Configuration.RowQueueType));
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));
        }
    }
}