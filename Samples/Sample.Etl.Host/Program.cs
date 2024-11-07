using FizzCode.EtLast;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = new HostApplicationBuilder();
builder.Services.AddLogging(x => x.ClearProviders());

builder.Services.AddEtlCommandService(() => new CommandService("Sample ETL Host")
    .AddCommandListener(typeof(ConsoleCommandListener))
    .SetAlias("test", "test-modules -a")
    .SetAlias("load", "run SampleModule Load")
    .ConfigureSession((builder, sessionArguments) => builder.UseRollingDevLogManifestFiles(1024))
    );

var host = builder.Build();
host.Run();
