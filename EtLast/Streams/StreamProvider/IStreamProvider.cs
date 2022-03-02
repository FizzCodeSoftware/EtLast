namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IStreamProvider
    {
        public string Topic { get; }
        public IEnumerable<NamedStream> GetStreams(IProcess caller);
    }
}