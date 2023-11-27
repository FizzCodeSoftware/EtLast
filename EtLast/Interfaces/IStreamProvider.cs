namespace FizzCode.EtLast;

public interface IStreamProvider
{
    public string GetTopic();
    public IEnumerable<NamedStream> GetStreams(IProcess caller);
}
