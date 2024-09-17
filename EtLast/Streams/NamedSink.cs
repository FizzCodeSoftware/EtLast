namespace FizzCode.EtLast;

public class NamedSink
{
    public string Name { get; }
    public Stream Stream { get; private set; }
    public IoCommand IoCommand { get; }

    public Sink Sink { get; }

    public NamedSink(string name, Stream stream, IoCommand ioCommand, Sink sink)
    {
        Name = name;
        Stream = stream;
        IoCommand = ioCommand;
        Sink = sink;
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

    public void Close()
    {
        Stream?.Flush();

        if (Stream != null)
        {
            Stream.Dispose();
            Stream = null;
        }
    }
}