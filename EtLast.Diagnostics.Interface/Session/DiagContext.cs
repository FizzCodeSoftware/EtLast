namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Name}")]
public class DiagContext
{
    public long Id { get; }
    public string Name { get; }

    public string DataDirectory { get; }

    public Playbook WholePlaybook { get; }
    public DateTime StartedOn { get; }
    public DateTime? EndedOn { get; protected set; }
    public bool FullyLoaded => EndedOn != null && _stagedEvents.Count == 0;
    public ContextIndex Index { get; }
    private readonly List<AbstractEvent> _stagedEvents = [];

    public DiagContext(long id, string name, DateTime startedOn, string dataDirectory)
    {
        Id = id;
        Name = name;
        WholePlaybook = new Playbook(this);
        StartedOn = startedOn;
        Index = new ContextIndex(dataDirectory);
    }

    public void Stage(MemoryStream input)
    {
        var events = Index.Append(input);
        lock (_stagedEvents)
        {
            _stagedEvents.AddRange(events);
        }
    }

    public void FlushToPlaybook()
    {
        List<AbstractEvent> newEvents;
        lock (_stagedEvents)
        {
            newEvents = _stagedEvents.ToList();
            _stagedEvents.Clear();
        }

        WholePlaybook.AddEvents(newEvents);
    }
}
