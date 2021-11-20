namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Data.Common;
    using FizzCode.LightWeight.AdoNet;

    public class Startup : IStartup
    {
        public void BuildSettings(IEnvironmentSettings settings)
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);

            settings.SetDevEnvironmentForInstance("WSDEVTWO");
            settings.FileLogSettings.MinimumLogLevel = LogSeverity.Debug;

            var connectionString = new NamedConnectionString("test", "System.Data.SqlClient", settings.GetConfigurationValue<string>("ConnectionString"), "2016");
            var databaseName = settings.GetConfigurationValue<string>("DatabaseName");

            settings.Commands.Add("main", () => new Main()
            {
                ConnectionString = connectionString,
                DatabaseName = databaseName,
            });

            settings.Commands.Add("createdb", () => new CreateDatabase()
            {
                ConnectionString = connectionString,
                Definition = new TestDwhDefinition(),
                DatabaseName = databaseName,
            });
        }
    }
}