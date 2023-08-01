namespace FizzCode.EtLast;

public class NamedSink : NamedStream
{
    public int SinkUid { get; }
    public long RowsWritten { get; private set; }

    private long CurrentPosition;

    public NamedSink(string name, Stream stream, int ioCommandUid, IoCommandKind ioCommandKind, int sinkUid, long startingPosition)
        : base(name, stream, ioCommandUid, ioCommandKind)
    {
        SinkUid = sinkUid;
        CurrentPosition = startingPosition;
    }

    public void IncreaseRowsWritten()
    {
        RowsWritten++;
    }

    public long SafeGetPosition()
    {
#pragma warning disable RCS1075 // Avoid empty catch clause that catches System.Exception.
        try
        {
            CurrentPosition = Stream.Position;
        }
        catch (Exception)
        {
        }
#pragma warning restore RCS1075 // Avoid empty catch clause that catches System.Exception.

        return CurrentPosition;
    }
}