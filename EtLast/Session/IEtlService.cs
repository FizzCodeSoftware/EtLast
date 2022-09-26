namespace FizzCode.EtLast;

public interface IEtlService
{
    public IEtlContext Context { get; }
    public void Start(IEtlContext context);
    public void Stop();
}
