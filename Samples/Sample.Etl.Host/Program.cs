using FizzCode.EtLast;

return (int)new ConsoleHost("Sample ETL Host")
    .UseCommandListener(hostArguments => new ConsoleCommandListener())
    .SetAlias("test", "test-modules -a")
    .SetAlias("load", "run SampleModule Load")
    .ConfigureSession((builder, sessionArguments) => builder.UseRollingDevLogManifestFiles())
    .Run();
