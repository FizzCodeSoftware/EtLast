using System;
using FizzCode.EtLast;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var serviceName = "EtLastIntegrationTest";

var builder = new HostBuilder()
    .ConfigureServices(svc => svc
        .AddHostedService(sp => CreateEtLastHost(serviceName, sp.GetRequiredService<IHostLifetime>()))
    );

if (Microsoft.Extensions.Hosting.WindowsServices.WindowsServiceHelpers.IsWindowsService())
{
    builder.UseWindowsService(x => x.ServiceName = serviceName);
}
else
{
    builder.UseConsoleLifetime();
}

var appHost = builder.Build();
appHost.Run();

static WindowsConsoleHost CreateEtLastHost(string serviceName, IHostLifetime lifetime)
{
    return new WindowsConsoleHost("EtLast Integration Tests", serviceName, lifetime)
        .UseCommandListener(hostArgs =>
        {
            Console.WriteLine("list of automatically compiled host argument values:");
            foreach (var key in hostArgs.AllKeys)
            {
                var v = hostArgs.GetAs<string>(key);
                if (v != null)
                    Console.WriteLine("[" + key + "] = [" + v + "]");
            }

            return new ConsoleCommandListener();
        })
        .SetAlias("test", "test-modules AdoNetTests FlowTests")
        .SetAlias("ado", "run AdoNetTests Main")
        .SetAlias("flow", "run FlowTests Main")
        //.DisableSerilogForModules()
        //.DisableSerilogForCommands()

        .ConfigureSession((builder, sessionArguments) => builder.UseRollingDevLogManifestFiles(null))

        .IfInstanceIs("WSDEVONE", host => host
            .IfDebuggerAttached(host => host
                .RegisterEtlContextListener(context => new DiagnosticsHttpSender(context)
                {
                    MaxCommunicationErrorCount = 2,
                    Url = "http://localhost:8642",
                }))
            );
}