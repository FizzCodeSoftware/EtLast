namespace FizzCode.EtLast;

public abstract class AbstractEtlService : IEtlService
{
    public IEtlContext Context { get; private set; }

    public void Start(IEtlContext context)
    {
        Context = context;
        OnStart();
    }

    public void Stop()
    {
        OnStop();
    }

    protected abstract void OnStart();
    protected abstract void OnStop();
}