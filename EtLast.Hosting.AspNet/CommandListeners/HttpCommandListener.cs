namespace FizzCode.EtLast;

public abstract class HttpCommandListener : ICommandListener
{
    public void Listen(ICommandService commandService, CancellationToken cancellationToken)
    {
        var mutexName = GetType().AssemblyQualifiedName + "|" + Environment.ProcessPath.Replace(Path.DirectorySeparatorChar, '_');
        var mutex = new Mutex(false, mutexName, out var _);

        var exists = false;
        var abandoned = false;
        var mustRelease = false;
        try
        {
            try
            {
                if (mutex.WaitOne(100, false))
                {
                    mustRelease = true;
                }
                else
                {
                    exists = true;
                }
            }
            catch (AbandonedMutexException)
            {
                abandoned = true;
                mustRelease = true;
            }

            if (exists)
            {
                commandService.Logger.Write(LogEventLevel.Information, "listening for http requests is skipped because another listener is already running...");
                return;
            }

            if (abandoned)
            {
                commandService.Logger.Write(LogEventLevel.Information, "abandoned http listener mutex detected (and ignored)");
            }

            commandService.Logger.Write(LogEventLevel.Information, "listening for http requests...");
            var app = CreateApplication(commandService);
            if (app == null)
                return;

            commandService.CancellationToken.Register(() => app.StopAsync().Wait());
            cancellationToken.Register(() => app.StopAsync().Wait());

            try
            {
                app.Run();
            }
            catch (Exception ex)
            {
                commandService.Logger.Error(ex, "error during the execution of the http listener application");
            }
        }
        finally
        {
            if (mustRelease)
            {
                commandService.Logger.Write(LogEventLevel.Information, "releasing http listener mutex...");
                mutex.ReleaseMutex();
                commandService.Logger.Write(LogEventLevel.Information, "http listener mutex released");
            }
        }
    }

    protected abstract WebApplication CreateApplication(ICommandService host);
}