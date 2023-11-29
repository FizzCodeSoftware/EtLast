namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Path}")]
public class TrackedSink(long id, string location, string path)
{
    public long Id { get; } = id;
    public string Location { get; } = location;
    public string Path { get; } = path;
    public long RowCount { get; set; }
}
