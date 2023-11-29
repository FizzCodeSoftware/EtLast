namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Id}")]
public class TrackedEtlRow
{
    public long Id { get; set; }
    public TrackedProcessInvocation CreatorProcess { get; set; }
    public TrackedProcessInvocation PreviousProcess { get; set; }
    public TrackedProcessInvocation NextProcess { get; set; }

    public List<AbstractRowEvent> AllEvents { get; } = [];

    public Dictionary<string, object> PreviousValues { get; set; }
    public Dictionary<string, object> NewValues { get; set; }
}
