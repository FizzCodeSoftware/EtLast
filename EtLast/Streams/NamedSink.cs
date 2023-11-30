namespace FizzCode.EtLast;

public class NamedSink(string name, Stream stream, IoCommand ioCommand, Sink sink) : NamedStream(name, stream, ioCommand)
{
    public Sink Sink { get; } = sink;
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