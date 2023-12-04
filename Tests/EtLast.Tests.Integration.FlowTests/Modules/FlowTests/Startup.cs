namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Startup : IStartup
{
    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks => [];

    public void Configure(EnvironmentSettings settings, IArgumentCollection arguments)
    {
        settings.FileLogSettings.MinimumLogLevel = LogSeverity.Verbose;
        settings.ConsoleLogSettings.MinimumLogLevel = LogSeverity.Verbose;
    }
}