namespace FizzCode.EtLast;

public sealed class EtlSession : IEtlSession
{
    public string Id { get; }
    public IEtlContext Context { get; }
    public ArgumentCollection Arguments { get; }

    private readonly List<IEtlService> _services = new();

    public EtlSession(string id, ArgumentCollection arguments)
    {
        Id = id;
        Context = new EtlContext();
        Arguments = arguments;
    }

    public T Service<T>() where T : IEtlService, new()
    {
        var service = _services.OfType<T>().FirstOrDefault();
        if (service != null)
            return service;

        service = new T();
        service.Start(this);
        _services.Add(service);

        return service;
    }

    public void Stop()
    {
        foreach (var service in _services)
        {
            service.Stop();
        }

        _services.Clear();
    }
}