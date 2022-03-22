namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IStreamProvider
    {
        public string GetTopic();
        public IEnumerable<NamedStream> GetStreams(IProcess caller);
    }
}