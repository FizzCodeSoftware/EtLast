namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Transactions;

    public class SplitProcess<TRowQueue> : ISplitProcess
        where TRowQueue : IRowQueue, new()
    {
        public IEtlContext Context { get; set; }
        public string Name { get; set; }
        public IProcess Caller { get; set; }
        public IProcess InputProcess { get; set; }

        private TRowQueue _queue;
        private Thread _feederThread;
        private readonly object _lock = new object();

        public SplitProcess(IEtlContext context, string name = null)
        {
            Context = context ?? throw new InvalidProcessParameterException(this, nameof(context), context, InvalidOperationParameterException.ValueCannotBeNullMessage);
            Name = name ?? GetType().Name;
        }

        public IEnumerable<IRow> Evaluate(IProcess caller)
        {
            // this process has multiple callers
            // Caller = caller;

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

            var rows = InputProcess.Evaluate(this);
            foreach (var row in rows)
            {
                _queue.AddRow(row);
            }

            _queue.SignalNoMoreRows();

            _queue = default(TRowQueue);
            _feederThread = null;
        }
    }
}