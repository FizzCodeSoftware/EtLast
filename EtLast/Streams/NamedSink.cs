namespace FizzCode.EtLast;

public class NamedSink(string name, Stream stream, long ioCommandId, IoCommandKind ioCommandKind, long sinkId) : NamedStream(name, stream, ioCommandId, ioCommandKind)
{
    public long SinkId { get; } = sinkId;
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