namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public sealed class Splitter<TRowQueue> : AbstractEvaluable
        where TRowQueue : IRowQueue, new()
    {
        public IProducer InputProcess { get; set; }
        public override bool ConsumerShouldNotBuffer => InputProcess?.ConsumerShouldNotBuffer == true;

        private TRowQueue _queue;
        private Thread _feederThread;
        private readonly object _lock = new();
        private bool _finished;

        public Splitter(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            StartQueueFeeder();
            return _queue.GetConsumer(Context.CancellationTokenSource.Token);
        }

        private void StartQueueFeeder()
        {
            lock (_lock)
            {
                if (_queue == null && !_finished)
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

            var rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership();

            foreach (var row in rows)
            {
                _queue.AddRow(row);
            }

            _queue.SignalNoMoreRows();

            lock (_lock)
            {
                _finished = true;
                _feederThread = null;
            }

            Context.RegisterProcessInvocationEnd(this);
        }
    }
}