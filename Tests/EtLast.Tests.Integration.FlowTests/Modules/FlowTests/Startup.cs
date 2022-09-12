namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Startup : IStartup
{
    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks => new()
    {
    };

    public void Configure(EnvironmentSettings settings)
    {
        settings.FileLogSettings.MinimumLogLevel = LogSeverity.Verbose;
        settings.ConsoleLogSettings.MinimumLogLevel = LogSeverity.Verbose;
    }
}