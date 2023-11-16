namespace FizzCode.EtLast;

public class NamedSink(string name, Stream stream, long ioCommandUid, IoCommandKind ioCommandKind, long sinkUid) : NamedStream(name, stream, ioCommandUid, ioCommandKind)
{
    public long SinkUid { get; } = sinkUid;
    public long RowsWritten { get; private set; }

    public void IncreaseRowsWritten(int count = 1)
    {
        RowsWritten += count;
    }

    public long SafeGetPosition()
    {
        try
        {
            return Stream.Position;
        }
        catch (Exception)
        {
        }

        return 0;
    }
}