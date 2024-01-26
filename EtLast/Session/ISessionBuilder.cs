namespace FizzCode.EtLast;

public interface ISessionBuilder
{
    public IEtlContext Context { get; }
    public string[] TaskNames { get; }
    public string ModuleDirectoryName { get; }
    public string TasksDirectoryName { get; }
    public string DevLogDirectory { get; }
    public string OpsLogDirectory { get; }

    public ISessionBuilder AddManifestProcessor(IManifestProcessor manifestProcessor);
    public ISessionBuilder UseTransactionScopeTimeout(TimeSpan timeout);
}