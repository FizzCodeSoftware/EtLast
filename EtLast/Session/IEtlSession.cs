namespace FizzCode.EtLast;

public interface IEtlSession
{
    public string Id { get; }
    public IEtlContext Context { get; }
    public ArgumentCollection Arguments { get; }

    public T Service<T>() where T : IEtlService, new();
}