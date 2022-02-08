namespace FizzCode.EtLast
{
    public interface IStreamSource
    {
        public string Topic { get; }
        public NamedStream GetStream(IProcess caller);
    }
}