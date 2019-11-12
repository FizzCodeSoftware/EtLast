namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public class SplitProcess<TRowQueue> : AbstractExecutableProcess, ISplitProcess
        where TRowQueue : IRowQueue, new()
    {
        public IEvaluable InputProcess { get; set; }
        public bool ConsumerShouldNotBuffer => InputProcess?.ConsumerShouldNotBuffer == true;

        private TRowQueue _queue;
        private Thread _feederThread;
        private readonly object _lock = new object();

        public SplitProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));
        }

        public IEnumerable<IRow> Evaluate(IProcess caller)
        {
            // this process has multiple callers
            Caller = caller;

            Validate();

            StartQueueFeeder();

            return _queue.GetConsumer(Context.CancellationTokenSource.Token);
        }

        protected override void ExecuteImpl()
        {
            throw new EtlException(nameof(Execute) + " is not supported in " + GetType().Name);
        }

        private void StartQueueFeeder()
        {
            lock (_lock)
            {
                if (_queue == null)
                {
                    _queue = new TRowQueue();

                    _feederThread = new Thread(QueueFeederWorker);
                    _feederThread.Start(Transaction.Current);
                }
            }
        }

        private void QueueFeederWorker(object tran)
        {
            Transaction.Current = tran as Transaction;

            var rows = InputProcess.Evaluate(this);
            foreach (var row in rows)
            {
                _queue.AddRow(row);
            }

            _queue.SignalNoMoreRows();

            _queue = default;
            _feederThread = null;
        }
    }
}