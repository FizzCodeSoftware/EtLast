namespace FizzCode.EtLast;

public interface IPartitionedSinkProvider
{
    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat, string[] columns);
    public bool AutomaticallyDispose { get; }
}
