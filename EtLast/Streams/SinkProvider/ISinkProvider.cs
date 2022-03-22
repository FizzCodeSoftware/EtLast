namespace FizzCode.EtLast
{
    public interface ISinkProvider
    {
        public void Validate(IProcess caller);
        public NamedSink GetSink(IProcess caller, string partitionKey);
        public bool AutomaticallyDispose { get; }
    }
}