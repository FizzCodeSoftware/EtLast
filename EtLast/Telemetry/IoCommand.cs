namespace FizzCode.EtLast;

public class IoCommand
{
    public long Id { get; internal set; }
    public IEtlContext Context { get; internal set; }

    public required IoCommandKind Kind { get; init; }
    public required IProcess Process { get; init; }

    public int? TimeoutSeconds { get; init; }
    public string Location { get; init; }
    public string Command { get; init; }
    public string Path { get; init; }
    public string TransactionId { get; init; }

    public Func<IEnumerable<KeyValuePair<string, object>>> ArgumentListGetter { get; init; }
    public string Message { get; init; }
    public string MessageExtra { get; init; }

    public long? AffectedDataCount { get; set; }
    public Exception Exception { get; private set; }

    public void End()
    {
        foreach (var listener in Context.Listeners)
            listener.OnContextIoCommandEnd(this);
    }

    public void Failed(Exception ex)
    {
        Exception = ex;
        End();
    }
}