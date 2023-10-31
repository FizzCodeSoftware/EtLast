namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Uid}")]
public class TrackedEtlRow
{
    public long Uid { get; set; }
    public TrackedProcessInvocation CreatorProcess { get; set; }
    public TrackedProcessInvocation PreviousProcess { get; set; }
    public TrackedProcessInvocation NextProcess { get; set; }

    public List<AbstractRowEvent> AllEvents { get; } = new List<AbstractRowEvent>();

    public Dictionary<string, object> PreviousValues { get; set; }
    public Dictionary<string, object> NewValues { get; set; }
}
