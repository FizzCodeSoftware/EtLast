namespace FizzCode.EtLast;

public class NamedSink(string name, Stream stream, IoCommand ioCommand, Sink sink)
    : NamedStream(name, stream, ioCommand)
{
    public Sink Sink { get; } = sink;

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

    public override void Close()
    {
        Stream?.Flush();
        base.Close();
    }
}