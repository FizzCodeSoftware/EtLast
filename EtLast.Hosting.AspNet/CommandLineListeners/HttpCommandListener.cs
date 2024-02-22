using System;
using System.Threading;
using System.Threading.Tasks;
using FizzCode.EtLast.Host;
using Microsoft.AspNetCore.Builder;

namespace FizzCode.EtLast;

public abstract class HttpCommandListener : ICommandListener
{
    public void Listen(IHost host, CancellationToken cancellationToken)
    {
        Console.WriteLine("listening for http requests...");
#pragma warning disable CA2016 // Forward the 'CancellationToken' parameter to methods
        Start(host, cancellationToken).Wait();
#pragma warning restore CA2016 // Forward the 'CancellationToken' parameter to methods
    }

    private async Task Start(IHost host, CancellationToken cancellationToken)
    {
        var app = await CreateApplication(host);
        if (app == null)
            return;

        host.CancellationToken.Register(() => app.StopAsync().Wait());
        cancellationToken.Register(() => app.StopAsync().Wait());

        app.Run();
    }

    protected abstract Task<WebApplication> CreateApplication(IHost host);
}