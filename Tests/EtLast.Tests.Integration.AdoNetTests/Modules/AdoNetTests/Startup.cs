namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Startup : IStartup
{
    public void Configure(HostSessionSettings settings, IArgumentCollection arguments)
    {
        settings.UseSqlClient();
        settings.FileLogSettings.MinimumLogLevel = LogSeverity.Information;
        settings.ConsoleLogSettings.MinimumLogLevel = LogSeverity.Debug;
    }
}