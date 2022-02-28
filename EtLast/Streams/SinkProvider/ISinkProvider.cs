namespace FizzCode.EtLast
{
    public interface ISinkProvider
    {
        public NamedSink GetSink(IProcess caller, string partitionKey);
        public bool AutomaticallyDispose { get; }
    }
}