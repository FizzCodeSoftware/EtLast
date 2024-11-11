namespace FizzCode.EtLast;

internal class IoCommandListener : IEtlContextListener
{
    public IEnumerable<IIoCommandCounter> Counters => _counters.Values;
    private readonly Dictionary<IoCommandKind, IoCommandCounter> _counters = [];

    public void Start()
    {
    }

    public void OnContextIoCommandStart(IoCommand ioCommand)
    {
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
    {
        _counters.TryGetValue(ioCommand.Kind, out var counter);
        if (counter == null)
        {
            _counters[ioCommand.Kind] = counter = new IoCommandCounter()
            {
                Kind = ioCommand.Kind,
            };
        }

        counter.InvocationCount++;

        if (ioCommand.AffectedDataCount != null)
        {
            var cnt = (counter.AffectedDataCount ?? 0) + ioCommand.AffectedDataCount.Value;
            counter.AffectedDataCount = cnt;
        }
    }

    public void OnContextClosed()
    {
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
    {
    }

    public void OnException(IProcess process, Exception exception)
    {
    }

    public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
    }

    public void OnProcessEnd(IProcess process)
    {
    }

    public void OnProcessStart(IProcess process)
    {
    }

    public void OnWriteToSink(Sink sink, IReadOnlyRow row)
    {
    }

    public void OnSinkStarted(IProcess process, Sink sink)
    {
    }
}