using FizzCode.EtLast;

return (int)ConsoleHostBuilder.New("Sample ETL Host")
    .HandleCommandLineArgs(args)
    .UseCommandLineListener(hostArguments => new ConsoleCommandLineListener())
    .SetAlias("test", "test-modules -a")
    .SetAlias("load", "run SampleModule Load")
    .Build()
    .Run();
