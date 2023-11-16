namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Path}")]
public class TrackedSink(long uid, string location, string path)
{
    public long UID { get; } = uid;
    public string Location { get; } = location;
    public string Path { get; } = path;
    public long RowCount { get; set; }
}
