namespace FizzCode.EtLast
{
    public interface IStreamProvider
    {
        public string Topic { get; }
        public NamedStream GetStream(IProcess caller);
    }
}