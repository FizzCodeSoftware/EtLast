namespace FizzCode.EtLast;

public class SessionBuilder : ISessionBuilder
{
    public required IEtlContext Context { get; init; }
    public required IEtlTask[] Tasks { get; init; }
    public required string TasksDirectoryName { get; init; }
    public required string DevLogDirectory { get; init; }
    public required string OpsLogDirectory { get; init; }

    public List<IManifestProcessor> ManifestProcessors { get; } = [];

    public ISessionBuilder AddManifestProcessor(IManifestProcessor manifestProcessor)
    {
        ManifestProcessors.Add(manifestProcessor);
        return this;
    }

    public ISessionBuilder UseTransactionScopeTimeout(TimeSpan timeout)
    {
        Context.TransactionScopeTimeout = timeout;
        return this;
    }
}