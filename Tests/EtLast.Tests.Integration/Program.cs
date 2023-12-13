using System;
using FizzCode.EtLast;

return (int)ConsoleHostBuilder.New("EtLast Integration Tests")
    .HandleCommandLineArgs(args)
    .UseCommandLineListener(hostArgs =>
    {
        Console.WriteLine("list of automatically compiled host argument values:");
        foreach (var key in hostArgs.AllKeys)
        {
            var v = hostArgs.GetAs<string>(key);
            if (v != null)
                Console.WriteLine("[" + key + "] = [" + v + "]");
        }

        return new ConsoleCommandLineListener();
    })
    .SetAlias("test", "test-modules AdoNetTests FlowTests")
    .SetAlias("ado", "run AdoNetTests Main")
    .SetAlias("flow", "run FlowTests Main")
    //.DisableSerilogForModules()
    //.DisableSerilogForCommands()
    .IfDebuggerAttached(b => b
        .RegisterEtlContextListener(context => new DiagnosticsHttpSender(context)
        {
            MaxCommunicationErrorCount = 2,
            Url = "http://localhost:8642",
        }))
    .Build()
    .Run();
