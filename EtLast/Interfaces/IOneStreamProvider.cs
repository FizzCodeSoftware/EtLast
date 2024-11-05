namespace FizzCode.EtLast;

public interface IOneStreamProvider
{
    public NamedStream GetStream(IProcess caller);
}