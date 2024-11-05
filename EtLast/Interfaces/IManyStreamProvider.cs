namespace FizzCode.EtLast;

public interface IManyStreamProvider
{
    public IEnumerable<NamedStream> GetStreams(IProcess caller);
}
