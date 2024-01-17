namespace SampleHost.SampleModule;

public class Startup : IStartup
{
    public void BuildSession(ISessionBuilder session, IArgumentCollection arguments)
    {
        session
            .EnableMicrosoftSqlClient()
            .LogToConsole(LogSeverity.Debug)
            .LogDevToFile()
            .LogOpsToFile()
            .LogIoToFile();
    }

    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks => [];
}