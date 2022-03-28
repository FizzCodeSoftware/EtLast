namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Startup : IStartup
{
    public Dictionary<string, Func<IEtlSessionArguments, IEtlTask>> Commands => new()
    {
        ["ExceptionTestCommand"] = args => new ExceptionTest()
        {
            ExceptionType = typeof(InvalidOperationException),
            Message = "oops something went wrong",
        },
    };

    public void Configure(EnvironmentSettings settings)
    {
        DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", Microsoft.Data.SqlClient.SqlClientFactory.Instance);
        settings.FileLogSettings.MinimumLogLevel = LogSeverity.Warning;
        settings.ConsoleLogSettings.MinimumLogLevel = LogSeverity.Verbose;
    }
}
