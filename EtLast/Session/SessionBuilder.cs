namespace FizzCode.EtLast;

public class SessionBuilder : ISessionBuilder
{
    public required IEtlContext Context { get; init; }
    public required IEtlTask[] Tasks { get; init; }
    public required string TasksDirectoryName { get; init; }
    public required string DevLogDirectory { get; init; }
    public required string OpsLogDirectory { get; init; }

    public List<Func<IManifestProcessor>> ManifestProcessorCreators { get; } = [];

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<Func<IEtlContextLogger>> EtlContextLoggerCreators { get; } = [];

    public ISessionBuilder AddManifestProcessor(Func<IManifestProcessor> manifestProcessor)
    {
        ManifestProcessorCreators.Add(manifestProcessor);
        return this;
    }

    public ISessionBuilder AddLogger(Func<IEtlContextLogger> creator)
    {
        EtlContextLoggerCreators.Add(creator);
        return this;
    }

    public ISessionBuilder UseTransactionScopeTimeout(TimeSpan timeout)
    {
        Context.TransactionScopeTimeout = timeout;
        return this;
    }
}