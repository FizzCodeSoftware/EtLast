namespace FizzCode.EtLast;

using System.Collections.Generic;

public interface IStreamProvider
{
    public string GetTopic();
    public void Validate(IProcess caller);
    public IEnumerable<NamedStream> GetStreams(IProcess caller);
}
