namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Path}")]
public class TrackedSink
{
    public int UID { get; }
    public string Location { get; }
    public string Path { get; }
    public int RowCount { get; set; }

    public TrackedSink(int uid, string location, string path)
    {
        UID = uid;
        Location = location;
        Path = path;
    }
}
