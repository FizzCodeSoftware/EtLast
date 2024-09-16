namespace FizzCode.EtLast;

public class SinkRegistry
{
    private readonly List<NamedSink> _sinks = [];

    public IEnumerable<NamedSink> All => _sinks;

    public void Add(NamedSink sink)
    {
        _sinks.Add(sink);
    }
}