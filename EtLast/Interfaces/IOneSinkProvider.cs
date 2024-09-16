namespace FizzCode.EtLast;

public interface IOneSinkProvider
{
    public NamedSink GetSink(IProcess caller, string sinkFormat, string[] columns);
    public bool AutomaticallyDispose { get; }

    /// <summary>
    /// If set to a <see cref="SinkRegistry"/> then every returned sink is registered to that collection.
    /// </summary>
    public SinkRegistry SinkRegistry { get; init; }
}