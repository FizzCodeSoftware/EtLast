namespace FizzCode.EtLast;

public class NamedSink : NamedStream
{
    public int SinkUid { get; }
    public long RowsWritten { get; private set; }

    public NamedSink(string name, Stream stream, int ioCommandUid, IoCommandKind ioCommandKind, int sinkUid)
        : base(name, stream, ioCommandUid, ioCommandKind)
    {
        SinkUid = sinkUid;
    }

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