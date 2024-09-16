namespace FizzCode.EtLast;

public class SinkMetadataEnricher
{
    private readonly List<Sink> _sinks = [];
    public IEnumerable<Sink> All => _sinks;

    public required Dictionary<string, string> Metadata { get; set; }

    public void Enrich(Sink sink)
    {
        _sinks.Add(sink);
        foreach (var kvp in Metadata)
        {
            sink.SetMetadata(kvp.Key, kvp.Value);
        }
    }
}