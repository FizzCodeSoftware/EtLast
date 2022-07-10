using FizzCode.EtLast.ConsoleHost;

return (int)HostBuilder.New("EtLast Integration Tests")
    .HandleCommandLineArgs(args)
    .UseCommandLineListener(new ConsoleCommandLineListener())
    .SetAlias("do", "run DwhBuilderTests Main")
    .SetAlias("doex1", "run DwhBuilderTests CustomExceptionTest")
    .SetAlias("doex2", "run DwhBuilderTests ExceptionTest")
    .SetAlias("createdb", "run DwhBuilderTests CreateDatabase")
    .SetAlias("test", "test-modules DwhBuilderTests")
    .SetAlias("ado", "run AdoNetTests Main")
    //.DisableSerilogForModules()
    //.DisableSerilogForCommands()
    /*.RegisterEtlContextListener(session => new FizzCode.EtLast.Diagnostics.HttpSender(session)
    {
        MaxCommunicationErrorCount = 2,
        Url = "http://localhost:8642",
    })*/
    .Build()
    .Run();
