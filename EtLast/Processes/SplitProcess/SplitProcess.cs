namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Transactions;

    public class SplitProcess<TRowQueue> : AbstractEvaluableProcess
        where TRowQueue : IRowQueue, new()
    {
        public IEvaluable InputProcess { get; set; }
        public override bool ConsumerShouldNotBuffer => InputProcess?.ConsumerShouldNotBuffer == true;

        private TRowQueue _queue;
        private Thread _feederThread;
        private readonly object _lock = new object();

        public SplitProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            StartQueueFeeder();
            return _queue.GetConsumer(Context.CancellationTokenSource.Token);
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

            var rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership(this);
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