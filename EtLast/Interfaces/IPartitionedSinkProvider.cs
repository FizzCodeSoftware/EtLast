namespace FizzCode.EtLast;

public interface IPartitionedSinkProvider
{
    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat, string[] columns);
    public bool AutomaticallyDispose { get; }

    /// <summary>
    /// If set to a <see cref="SinkRegistry"/> then every returned sink is registered to that collection.
    /// </summary>
    public SinkRegistry SinkRegistry { get; init; }
}
