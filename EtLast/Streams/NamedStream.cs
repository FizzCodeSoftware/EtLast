namespace FizzCode.EtLast;

public class NamedStream
{
    public string Name { get; }
    public Stream Stream { get; private set; }
    public int IoCommandUid { get; }
    public IoCommandKind IoCommandKind { get; }

    public NamedStream(string name, Stream stream, int ioCommandUid, IoCommandKind ioCommandKind)
    {
        Name = name;
        Stream = stream;
        IoCommandUid = ioCommandUid;
        IoCommandKind = ioCommandKind;
    }

    public void Dispose()
    {
        if (Stream != null)
        {
            Stream.Dispose();
            Stream = null;
        }
    }
}