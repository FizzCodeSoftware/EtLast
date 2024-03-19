using System;
using FizzCode.EtLast;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = new HostApplicationBuilder();
builder.Services.AddLogging(x => x.ClearProviders());

builder.Services.AddEtLastCommandService(() => new WindowsConsoleEtlCommandService("EtLast Integration Tests", "EtLastIntegrationTest")
    .AddCommandListener(serviceArgs =>
    {
        Console.WriteLine("list of automatically compiled service argument values:");
        foreach (var key in serviceArgs.AllKeys)
        {
            var v = serviceArgs.GetAs<string>(key);
            if (v != null)
                Console.WriteLine("[" + key + "] = [" + v + "]");
        }

        return new ConsoleCommandListener();
    })
    .SetAlias("test", "test-modules AdoNetTests FlowTests")
    .SetAlias("ado", "run AdoNetTests Main")
    .SetAlias("flow", "run FlowTests Main")
    .ConfigureSession((builder, sessionArguments) => builder.UseRollingDevLogManifestFiles(null))
    );

var host = builder.Build();
host.Run();
