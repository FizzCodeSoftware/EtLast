namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Startup : IStartup
{
    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks => [];

    public void Configure(EnvironmentSettings settings)
    {
        DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", Microsoft.Data.SqlClient.SqlClientFactory.Instance);
        settings.FileLogSettings.MinimumLogLevel = LogSeverity.Information;
        settings.ConsoleLogSettings.MinimumLogLevel = LogSeverity.Debug;
    }
}