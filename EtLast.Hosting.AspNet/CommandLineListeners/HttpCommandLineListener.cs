using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;

namespace FizzCode.EtLast;

public abstract class HttpCommandLineListener : ICommandLineListener
{
    public void Listen(IHost host)
    {
        Console.WriteLine("listening for http requests...");
        Start(host).GetAwaiter().GetResult();
    }

    private async Task Start(IHost host)
    {
        var app = await CreateApplication(host);
        if (app == null)
            return;

        host.CancellationToken.Register(() => app.StopAsync().GetAwaiter().GetResult());

        app.Run();
    }

    protected abstract Task<WebApplication> CreateApplication(IHost host);
}