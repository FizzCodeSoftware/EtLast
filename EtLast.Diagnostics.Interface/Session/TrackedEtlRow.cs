namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Id}")]
public class TrackedEtlRow
{
    public long Id { get; set; }
    public TrackedProcess CreatorProcess { get; set; }
    public TrackedProcess PreviousProcess { get; set; }
    public TrackedProcess NextProcess { get; set; }

    public List<AbstractRowEvent> AllEvents { get; } = [];

    public Dictionary<string, object> PreviousValues { get; set; }
    public Dictionary<string, object> NewValues { get; set; }
}
