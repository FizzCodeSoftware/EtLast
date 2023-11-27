namespace FizzCode.EtLast;

public sealed class Splitter<TRowQueue> : AbstractSequence
    where TRowQueue : IRowQueue, new()
{
    public required ISequence InputProcess { get; init; }

    private TRowQueue _queue;
    private Thread _feederThread;
    private readonly object _lock = new();
    private bool _finished;

    protected override void ValidateImpl()
    {
        if (InputProcess == null)
            throw new ProcessParameterNullException(this, nameof(InputProcess));
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        StartQueueFeeder();
        return _queue.GetConsumer(Context.CancellationToken);
    }

    private void StartQueueFeeder()
    {
        lock (_lock)
        {
            if (_queue == null && !_finished)
            {
                _queue = new TRowQueue();

                _feederThread = new Thread(() => QueueFeederWorker(Transaction.Current));
                _feederThread.Start();
            }
        }
    }

    private void QueueFeederWorker(Transaction tran)
    {
        Transaction.Current = tran;

        // todo: multiple caller invocation contexts vs 1 input invocation context
        var rows = InputProcess.TakeRowsAndTransferOwnership(this);

        foreach (var row in rows)
        {
            if (FlowState.IsTerminating)
                break;

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