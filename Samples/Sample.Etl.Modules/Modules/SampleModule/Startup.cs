namespace SampleHost.SampleModule;

public class Startup : IStartup
{
    public void Configure(EnvironmentSettings settings, IArgumentCollection arguments)
    {
        System.Data.Common.DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", Microsoft.Data.SqlClient.SqlClientFactory.Instance);
    }

    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks => [];
}