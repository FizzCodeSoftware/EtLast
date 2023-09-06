#pragma warning disable RCS1075 // Avoid empty catch clause that catches System.Exception.
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
#pragma warning restore RCS1075 // Avoid empty catch clause that catches System.Exception.