namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Startup : IStartup
{
    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks => new()
    {
        ["CustomExceptionTest"] = args => new ExceptionTest()
        {
            ExceptionType = typeof(InvalidOperationException),
            Message = "oops something went wrong",
        },
    };

    public void Configure(EnvironmentSettings settings)
    {
        DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", Microsoft.Data.SqlClient.SqlClientFactory.Instance);
        settings.FileLogSettings.MinimumLogLevel = LogSeverity.Information;
        settings.ConsoleLogSettings.MinimumLogLevel = LogSeverity.Information;
    }
}