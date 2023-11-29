namespace FizzCode.EtLast;

public class NamedStream(string name, Stream stream, long ioCommandId, IoCommandKind ioCommandKind)
{
    public string Name { get; } = name;
    public Stream Stream { get; private set; } = stream;
    public long IoCommandId { get; } = ioCommandId;
    public IoCommandKind IoCommandKind { get; } = ioCommandKind;
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