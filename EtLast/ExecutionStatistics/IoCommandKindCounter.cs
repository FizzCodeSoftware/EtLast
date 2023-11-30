namespace FizzCode.EtLast;

public class IoCommandKindCounter : IEtlContextListener
{
    public IReadOnlyDictionary<IoCommandKind, IoCommandCounter> Counters => _counters;
    private readonly Dictionary<IoCommandKind, IoCommandCounter> _counters = [];

    public void Start()
    {
    }

    public void OnContextIoCommandStart(IProcess process, IoCommand ioCommand)
    {
    }

    public void OnContextIoCommandEnd(IProcess process, IoCommand ioCommand)
    {
        _counters.TryGetValue(ioCommand.Kind, out var counter);
        if (counter == null)
        {
            _counters[ioCommand.Kind] = counter = new IoCommandCounter();
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

    public void OnProcessInvocationEnd(IProcess process)
    {
    }

    public void OnProcessInvocationStart(IProcess process)
    {
    }

    public void OnRowCreated(IReadOnlyRow row)
    {
    }

    public void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess)
    {
    }

    public void OnWriteToSink(IReadOnlyRow row, long sinkId)
    {
    }

    public void OnSinkStarted(long sinkId, string location, string path, string sinkFormat, Type sinkWriter)
    {
    }

    public void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values)
    {
    }
}