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
    public List<Func<IEtlContextLogger>> ContextLoggerCreators { get; } = [];

    public ISessionBuilder AddManifestProcessor(Func<IManifestProcessor> manifestProcessor)
    {
        ManifestProcessorCreators.Add(manifestProcessor);
        return this;
    }

    public ISessionBuilder AddContextLogger(Func<IEtlContextLogger> creator)
    {
        ContextLoggerCreators.Add(creator);
        return this;
    }

    public ISessionBuilder UseTransactionScopeTimeout(TimeSpan timeout)
    {
        Context.TransactionScopeTimeout = timeout;
        return this;
    }

    public ISessionBuilder RemoveAllManifestProcessors()
    {
        ManifestProcessorCreators.Clear();
        return this;
    }

    public ISessionBuilder RemoveAllContextLoggers()
    {
        ContextLoggerCreators.Clear();
        return this;
    }
}