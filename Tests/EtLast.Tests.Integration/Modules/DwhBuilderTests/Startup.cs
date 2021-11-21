namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Data.Common;
    using FizzCode.LightWeight.AdoNet;

    public class Startup : IStartup
    {
        public void BuildSettings(IEnvironmentSettings environment)
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);

            environment.SetDevEnvironmentForInstance("WSDEVTWO");
            environment.FileLogSettings.MinimumLogLevel = LogSeverity.Debug;

            var connectionString = new NamedConnectionString("test", "System.Data.SqlClient", environment.GetConfigurationValue<string>("ConnectionString"), "2016");
            var databaseName = environment.GetConfigurationValue<string>("DatabaseName");

            environment.Commands.Add("main", () => new Main()
            {
                ConnectionString = connectionString,
                DatabaseName = databaseName,
            });

            environment.Commands.Add("createdb", () => new CreateDatabase()
            {
                ConnectionString = connectionString,
                Definition = new TestDwhDefinition(),
                DatabaseName = databaseName,
            });
        }
    }
}