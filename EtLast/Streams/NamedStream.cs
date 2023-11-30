namespace FizzCode.EtLast;

public class NamedStream(string name, Stream stream, IoCommand ioCommand)
{
    public string Name { get; } = name;
    public Stream Stream { get; private set; } = stream;
    public IoCommand IoCommand { get; } = ioCommand;
    public EventHandler OnDispose { get; set; }

    public void Dispose()
    {
        OnDispose?.Invoke(this, EventArgs.Empty);

        if (Stream != null)
        {
            Stream.Dispose();
            Stream = null;
        }
    }
}