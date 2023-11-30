namespace FizzCode.EtLast;

public interface ISinkProvider
{
    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat);
    public bool AutomaticallyDispose { get; }
}
