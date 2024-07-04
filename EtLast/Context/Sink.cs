namespace FizzCode.EtLast;

public class Sink
{
    public long Id { get; internal init; }
    public IEtlContext Context { get; internal init; }

    public string Location { get; internal init; }
    public string Path { get; internal init; }
    public string Format { get; internal init; }
    public long ProcessId { get; internal init; }

    public string[] Columns { get; internal init; }

    public int RowsWritten { get; private set; }

    public void RegisterWrite(IReadOnlyRow row)
    {
        RowsWritten++;

        foreach (var listener in Context.Listeners)
            listener.OnWriteToSink(this, row);
    }
}