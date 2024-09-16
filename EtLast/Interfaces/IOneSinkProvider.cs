namespace FizzCode.EtLast;

public interface IOneSinkProvider
{
    public NamedSink GetSink(IProcess caller, string sinkFormat, string[] columns);
    public bool AutomaticallyDispose { get; }
    public SinkMetadataEnricher SinkMetadataEnricher { get; init; }
}