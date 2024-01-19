namespace FizzCode.EtLast;

public interface IManyStreamProvider
{
    public string GetTopic();
    public IEnumerable<NamedStream> GetStreams(IProcess caller);
}
