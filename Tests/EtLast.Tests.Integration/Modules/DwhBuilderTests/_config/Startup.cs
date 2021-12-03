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
            settings.ConsoleLogSettings.MinimumLogLevel = LogSeverity.Verbose;
        }

        public Dictionary<string, Func<IEtlSessionArguments, IEtlTask>> Commands => new()
        {
            ["ExceptionTestCommand"] = args => new ExceptionTest()
            {
                ExceptionType = typeof(InvalidOperationException),
            }
        };
    }
}