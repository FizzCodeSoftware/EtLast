using System;
using FizzCode.EtLast;
using Microsoft.Extensions.Hosting;

new HostBuilder()
    .EnableEtLast(() => new WindowsConsoleHost("EtLast Integration Tests", "EtLastIntegrationTest")
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
        .UseCommandListener(hostArgs => new LocalFileCommandListener()
        {
            CommandFilePath = @"h:\command.txt",
        })
        .SetAlias("test", "test-modules AdoNetTests FlowTests")
        .SetAlias("ado", "run AdoNetTests Main")
        .SetAlias("flow", "run FlowTests Main")
        .ConfigureSession((builder, sessionArguments) => builder.UseRollingDevLogManifestFiles(null))

        /*.IfInstanceIs("WSDEVONE", host => host
            .IfDebuggerAttached(host => host
                .RegisterEtlContextListener(context => new DiagnosticsHttpSender(context)
                {
                    MaxCommunicationErrorCount = 2,
                    Url = "http://localhost:8642",
                }))
            )*/
        )
    .Build()
    .Run();
