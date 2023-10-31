namespace FizzCode.EtLast;

public class NamedStream
{
    public string Name { get; }
    public Stream Stream { get; private set; }
    public long IoCommandUid { get; }
    public IoCommandKind IoCommandKind { get; }
    public EventHandler OnDispose { get; set; }

    public NamedStream(string name, Stream stream, long ioCommandUid, IoCommandKind ioCommandKind)
    {
        Name = name;
        Stream = stream;
        IoCommandUid = ioCommandUid;
        IoCommandKind = ioCommandKind;
    }

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