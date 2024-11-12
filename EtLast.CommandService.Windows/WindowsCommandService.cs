namespace FizzCode.EtLast;

public class WindowsCommandService : CommandService
{
    public string ServiceName { get; }

    private Semaphore StopAcrossProcessesSemaphore;
    private bool StopAcrossProcessesSemaphoreTriggered = false;

    public override bool ConsoleHidden { get; } = Microsoft.Extensions.Hosting.WindowsServices.WindowsServiceHelpers.IsWindowsService();

    public WindowsCommandService(string name, string serviceName)
        : base(name)
    {
        ServiceName = serviceName;
    }

    protected override void AfterInit()
    {
        var semaphoreName = @"Global\" + Environment.ProcessPath.Replace('\\', '_').ToLowerInvariant();
        var semaphoreSecurity = new SemaphoreSecurity();
        semaphoreSecurity.AddAccessRule(new SemaphoreAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), SemaphoreRights.FullControl, AccessControlType.Allow));
        StopAcrossProcessesSemaphore = SemaphoreAcl.Create(100, 100, semaphoreName, out var createdNew, semaphoreSecurity);

        Logger.Write(LogEventLevel.Debug, "interprocess semaphore is {CreatedOrTaken}: {Name}", createdNew ? "CREATED" : "TAKEN", semaphoreName);
    }

    protected override IExecutionResult RunCustomCommand(string commandId, string originalCommand, string[] commandParts)
    {
        switch (commandParts[0].ToLowerInvariant())
        {
            case "--stopallandwait":
                StopAllAndWait();
                return new ExecutionResult(ExecutionStatusCode.Success);
            case "--installsvc":
                InstallWindowsService("delayed-auto", null);
                return new ExecutionResult(ExecutionStatusCode.Success);
            case "--uninstallsvc":
                UninstallWindowsService();
                return new ExecutionResult(ExecutionStatusCode.Success);
        }

        return base.RunCustomCommand(commandId, originalCommand, commandParts);
    }

    private void StopAllAndWait()
    {
        Logger.Write(LogEventLevel.Debug, "triggering interprocess stop semaphore");
        for (var i = 0; i < 100; i++)
        {
            try
            {
                StopAcrossProcessesSemaphore.WaitOne(10);
            }
            catch { }
        }

        Logger.Write(LogEventLevel.Debug, "finished triggering interprocess stop semaphore");

        var selfProcess = Process.GetCurrentProcess();

        var loggedCount = 0;
        while (true)
        {
            var siblingProcessList = Process.GetProcesses().Where(x => ProcessIsSibling(x, selfProcess)).ToList();
            var siblingCount = siblingProcessList.Count - 1;
            if (siblingCount == 0)
                break;

            if (loggedCount != siblingCount)
            {
                Logger.Write(LogEventLevel.Information, "waiting for " + siblingCount.ToString() + " sibling processes to terminate gracefully...");
                loggedCount = siblingCount;
            }

            Thread.Sleep(100);
        }
    }

    private static bool ProcessIsSibling(Process process, Process selfProcess)
    {
        try
        {
            return process.ProcessName.Equals(selfProcess.ProcessName, StringComparison.InvariantCultureIgnoreCase) && process.MainModule.FileName.Equals(selfProcess.MainModule.FileName, StringComparison.InvariantCultureIgnoreCase);
        }
        catch { return false; }
    }

    protected override void InsideMainLoop()
    {
        if (StopAcrossProcessesSemaphoreTriggered)
            return;

        var success = false;
        try
        {
            success = StopAcrossProcessesSemaphore.WaitOne(1000);
            if (!success)
            {
                Logger.Write(LogEventLevel.Debug, "interprocess stop semaphore triggered");
                StopAcrossProcessesSemaphoreTriggered = true;
                StopAllCommandListeners();
            }
        }
        catch (Exception)
        {
        }
        finally
        {
            if (success)
                StopAcrossProcessesSemaphore.Release();
        }
    }

    private void InstallWindowsService(string startMode, string customPath)
    {
        if (!Environment.IsPrivilegedProcess)
        {
            Logger.Write(LogEventLevel.Warning, "restart the application with administrator privileges to perform this operation");
            return;
        }

        Process.Start(new ProcessStartInfo("sc", string.Format("delete {0}", ServiceName)));
        Process.Start(new ProcessStartInfo("sc", string.Format("create {0} start={1} displayname=\"{2}\" binpath=\"\"{3}\"\"", ServiceName, startMode, Name, customPath ?? Environment.ProcessPath)));
        //Process.Start(new ProcessStartInfo("sc", string.Format("failure {0} reset=86400 actions=restart/1000/restart/1000/restart/1000", name)));
    }

    private void UninstallWindowsService()
    {
        if (!Environment.IsPrivilegedProcess)
        {
            Logger.Write(LogEventLevel.Warning, "restart the application with administrator privileges to perform this operation");
            return;
        }

        Process.Start(new ProcessStartInfo("sc", string.Format("delete {0}", ServiceName)));
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class WindowsCommandServiceFluent
{
    public static IServiceCollection AddEtlWindowsCommandService(this IServiceCollection services, Func<WindowsCommandService> serviceCreator)
    {
        var service = serviceCreator.Invoke();
        services.AddHostedService(serviceProvider =>
        {
            var hostLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();
            service.HostLifetime = hostLifetime;
            return service;
        });

        if (Microsoft.Extensions.Hosting.WindowsServices.WindowsServiceHelpers.IsWindowsService())
        {
            services.AddWindowsService(x => x.ServiceName = service.ServiceName);
            services.Configure<HostOptions>(options =>
            {
                options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore;
                options.ShutdownTimeout = Timeout.InfiniteTimeSpan;
            });
        }

        return services;
    }
}