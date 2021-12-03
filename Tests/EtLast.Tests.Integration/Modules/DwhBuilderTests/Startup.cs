namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;

    public class Startup : IStartup
    {
        public void Configure(EnvironmentSettings settings)
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);
            settings.FileLogSettings.MinimumLogLevel = LogSeverity.Debug;
        }

        public Dictionary<string, Func<IEtlSessionArguments, IEtlTask>> Commands => new()
        {
            ["main"] = args => new Main(),
            ["createdb"] = args => new CreateDatabase()
            {
                Definition = new TestDwhDefinition(),
            },
        };
    }
}