namespace FizzCode.EtLast;

public interface ISessionBuilder
{
    public IEtlContext Context { get; }
    public IEtlTask[] Tasks { get; }
    public string TasksDirectoryName { get; }

    public string DevLogDirectory { get; }
    public string OpsLogDirectory { get; }

    public bool ConsoleHidden { get; }

    public ISessionBuilder AddManifestProcessor(Func<IManifestProcessor> manifestProcessor);
    public ISessionBuilder RemoveAllManifestProcessors();

    public ISessionBuilder AddContextLogger(Func<IEtlContextLogger> creator);
    public ISessionBuilder RemoveAllContextLoggers();

    public ISessionBuilder UseTransactionScopeTimeout(TimeSpan timeout);
}