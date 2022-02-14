namespace FizzCode.EtLast
{
    public interface ISinkProvider
    {
        public string Topic { get; }
        public NamedSink GetSink(IProcess caller);
        public bool AutomaticallyDispose { get; }
    }
}