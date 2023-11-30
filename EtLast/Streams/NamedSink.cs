namespace FizzCode.EtLast;

public class NamedSink(string name, Stream stream, IoCommand ioCommand, long sinkId) : NamedStream(name, stream, ioCommand)
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