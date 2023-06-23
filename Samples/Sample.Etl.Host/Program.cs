using FizzCode.EtLast.ConsoleHost;

return (int)HostBuilder.New("Sample ETL")
    .HandleCommandLineArgs(args)
    .UseCommandLineListener(hostArguments => new ConsoleCommandLineListener())
    .SetAlias("test", "test modules -a")
    .SetAlias("load", "run SampleModule Load")
    .Build()
    .Run();
