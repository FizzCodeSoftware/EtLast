namespace FizzCode.EtLast;

public class NamedStream
{
    public string Name { get; }
    public Stream Stream { get; private set; }
    public IoCommand IoCommand { get; }
    public EventHandler OnDispose { get; set; }

    public NamedStream(string name, Stream stream, IoCommand ioCommand)
    {
        Name = name;
        Stream = stream;
        IoCommand = ioCommand;
    }

    public virtual void Close()
    {
        OnDispose?.Invoke(this, EventArgs.Empty);

        if (Stream != null)
        {
            Stream.Dispose();
            Stream = null;
        }
    }
}