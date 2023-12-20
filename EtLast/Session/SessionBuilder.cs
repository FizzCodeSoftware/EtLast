namespace FizzCode.EtLast;

public class SessionBuilder : ISessionBuilder
{
    public required IEtlContext Context { get; init; }
    public required string[] TaskNames { get; init; }
    public required string ModuleFolderName { get; init; }
    public required string TasksFolderName { get; init; }
    public required string DevLogFolder { get; init; }
    public required string OpsLogFolder { get; init; }

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