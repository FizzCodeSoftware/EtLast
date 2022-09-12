using System;
using FizzCode.EtLast.ConsoleHost;

return (int)HostBuilder.New("EtLast Integration Tests")
    .HandleCommandLineArgs(args)
    .UseCommandLineListener(hostArgs =>
    {
        Console.WriteLine("list of automatically compiled host argument values:");
        foreach (var kvp in hostArgs.All)
        {
            var v = hostArgs.Get<string>(kvp.Key);
            if (v != null)
                Console.WriteLine("[" + kvp.Key + "] = [" + v + "]");
        }

        return new ConsoleCommandLineListener();
    })
    .SetAlias("dwh", "run DwhBuilderTests Main")
    .SetAlias("doex1", "run DwhBuilderTests CustomExceptionTest")
    .SetAlias("doex2", "run DwhBuilderTests ExceptionTest")
    .SetAlias("createdb", "run DwhBuilderTests CreateDatabase")
    .SetAlias("test", "test-modules DwhBuilderTests")
    .SetAlias("ado", "run AdoNetTests Main")
    .SetAlias("flow", "run FlowTests Main")
    //.DisableSerilogForModules()
    //.DisableSerilogForCommands()
    /*.RegisterEtlContextListener(session => new FizzCode.EtLast.Diagnostics.HttpSender(session)
    {
        MaxCommunicationErrorCount = 2,
        Url = "http://localhost:8642",
    })*/
    .Build()
    .Run();
