namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Startup : IStartup
{
    public void BuildSession(ISessionBuilder session, IArgumentCollection arguments)
    {
        session
            .EnableSqlClient()
            .LogToConsole(LogSeverity.Debug)
            .LogDevToFile()
            .LogOpsToFile()
            .LogIoToFile();
    }
}