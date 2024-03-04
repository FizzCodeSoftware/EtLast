namespace FizzCode.EtLast;

public class WindowsConsoleHost : ConsoleHost
{
    public string ServiceName { get; }

    public WindowsConsoleHost(string name, string serviceName)
        : base(name)
    {
        ServiceName = serviceName;
    }

    protected override IExecutionResult RunCustomCommand(string commandId, string[] commandParts)
    {
        switch (commandParts[0].ToLowerInvariant())
        {
            case "--installsvc":
                InstallWindowsService(ServiceName, Name, "delayed-auto");
                return new ExecutionResult(ExecutionStatusCode.Success);
            case "--uninstallsvc":
                UninstallWindowsService(ServiceName);
                return new ExecutionResult(ExecutionStatusCode.Success);
        }

        return base.RunCustomCommand(commandId, commandParts);
    }

    private void InstallWindowsService(string name, string displayName, string startMode, string customPath = null)
    {
        if (!Environment.IsPrivilegedProcess)
        {
            Console.WriteLine("restart the application with administrator privileges to perform this operation");
            return;
        }

        Process.Start(new ProcessStartInfo("sc", string.Format("delete {0}", name)));
        Process.Start(new ProcessStartInfo("sc", string.Format("create {0} start={1} displayname=\"{2}\" binpath=\"\"{3}\"\"", name, startMode, displayName, customPath ?? Environment.ProcessPath)));
        Process.Start(new ProcessStartInfo("sc", string.Format("failure {0} reset=86400 actions=restart/1000/restart/1000/restart/1000", name)));
    }

    private void UninstallWindowsService(string name)
    {
        if (!Environment.IsPrivilegedProcess)
        {
            Console.WriteLine("restart the application with administrator privileges to perform this operation");
            return;
        }

        Process.Start(new ProcessStartInfo("sc", string.Format("delete {0}", name)));
    }
}