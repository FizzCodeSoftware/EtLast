namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Startup : IStartup
{
    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks => [];

    public void BuildSession(ISessionBuilder builder, IArgumentCollection arguments)
    {
        builder
            .LogToConsole(LogSeverity.Verbose)
            .LogDevToFile(LogSeverity.Verbose);
    }
}