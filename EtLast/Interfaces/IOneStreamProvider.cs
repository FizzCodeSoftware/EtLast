namespace FizzCode.EtLast;

public interface IOneStreamProvider
{
    public string GetTopic();
    public NamedStream GetStream(IProcess caller);
}