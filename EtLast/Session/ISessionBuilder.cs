namespace FizzCode.EtLast;

public interface ISessionBuilder
{
    public IEtlContext Context { get; }
    public string[] TaskNames { get; }
    public string ModuleFolderName { get; }
    public string TasksFolderName { get; }
    public string DevLogFolder { get; }
    public string OpsLogFolder { get; }

    public ISessionBuilder AddManifestProcessor(IManifestProcessor manifestProcessor);
    public ISessionBuilder UseTransactionScopeTimeout(TimeSpan timeout);
}