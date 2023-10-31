namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Path}")]
public class TrackedSink
{
    public long UID { get; }
    public string Location { get; }
    public string Path { get; }
    public long RowCount { get; set; }

    public TrackedSink(long uid, string location, string path)
    {
        UID = uid;
        Location = location;
        Path = path;
    }
}
