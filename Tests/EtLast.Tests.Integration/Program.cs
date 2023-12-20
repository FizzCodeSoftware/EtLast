using System;
using FizzCode.EtLast;

return (int)new ConsoleHost("EtLast Integration Tests")
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
    .IfDebuggerAttached(host => host
        .RegisterEtlContextListener(context => new DiagnosticsHttpSender(context)
        {
            MaxCommunicationErrorCount = 2,
            Url = "http://localhost:8642",
        }))
    .Run();
