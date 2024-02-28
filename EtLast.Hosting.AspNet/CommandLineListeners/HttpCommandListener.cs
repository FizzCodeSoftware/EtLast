namespace FizzCode.EtLast;

public abstract class HttpCommandListener : ICommandListener
{
    public void Listen(IHost host, CancellationToken cancellationToken)
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
                host.Logger.Write(LogEventLevel.Information, "listening for http requests is skipped because another listener is already running...");
                return;
            }

            if (abandoned)
            {
                host.Logger.Write(LogEventLevel.Information, "abandoned http listener mutex detected (and ignored)");
            }

            host.Logger.Write(LogEventLevel.Information, "listening for http requests...");
            var app = CreateApplication(host);
            if (app == null)
                return;

            host.CancellationToken.Register(() => app.StopAsync().Wait());
            cancellationToken.Register(() => app.StopAsync().Wait());

            try
            {
                app.Run();
            }
            catch (Exception ex)
            {
                host.Logger.Error(ex, "error during the execution of the http listener application");
            }

            //Thread.Sleep(60000);
        }
        finally
        {
            if (mustRelease)
            {
                host.Logger.Write(LogEventLevel.Information, "releasing http listener mutex...");
                mutex.ReleaseMutex();
                host.Logger.Write(LogEventLevel.Information, "http listener mutex released");
            }
        }
    }

    protected abstract WebApplication CreateApplication(IHost host);
}