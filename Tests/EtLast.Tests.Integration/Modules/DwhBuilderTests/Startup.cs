namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using FizzCode.LightWeight.AdoNet;

    public class Startup : IStartup
    {
        public void Configure(EnvironmentSettings settings)
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);
            settings.FileLogSettings.MinimumLogLevel = LogSeverity.Debug;
        }

        public Dictionary<string, Func<IEtlSessionArguments, IEtlTask>> Commands => new()
        {
            ["main"] = args => new Main()
            {
                ConnectionString = new NamedConnectionString("test", "System.Data.SqlClient", args.Get<string>("ConnectionString"), "2016"),
                DatabaseName = args.Get<string>("DatabaseName"),
            },
            ["createdb"] = args => new CreateDatabase()
            {
                ConnectionString = new NamedConnectionString("test", "System.Data.SqlClient", args.Get<string>("ConnectionString"), "2016"),
                Definition = new TestDwhDefinition(),
                DatabaseName = args.Get<string>("DatabaseName"),
            },
        };
    }
}