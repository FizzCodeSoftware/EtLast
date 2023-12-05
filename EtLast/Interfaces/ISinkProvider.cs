namespace FizzCode.EtLast;

public interface ISinkProvider
{
    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat, string[] columns);
    public bool AutomaticallyDispose { get; }
}
