namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Row}")]
public class StoredRow
{
    public int Id { get; set; }
    public TrackedSink Store { get; set; }
    public TrackedProcessInvocation Process { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
