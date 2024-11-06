using FizzCode.EtLast;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = new HostApplicationBuilder();
builder.Services.AddLogging(x => x.ClearProviders());

builder.Services.AddEtlCommandService(() => new WindowsCommandService("EtLast Integration Tests", "EtLastIntegrationTest")
    .AddCommandListener(service =>
    {
        service.Logger.Debug("list of automatically compiled service argument values:");
        foreach (var key in service.ServiceArguments.AllKeys)
        {
            var v = service.ServiceArguments.GetAs<string>(key);
            if (v != null)
                service.Logger.Debug("[" + key + "] = [" + v + "]");
        }

        return new ConsoleCommandListener();
    })
    /*.AddCommandListener(args => new LocalFileCommandListener()
    {
        CommandFilePath = @"h:\command.txt",
    })*/
    .SetAlias("test", "test-modules AdoNetTests FlowTests")
    .SetAlias("ado", "run AdoNetTests Main")
    .SetAlias("flow", "run FlowTests Main")
    .ConfigureSession((builder, sessionArguments) => builder.UseRollingDevLogManifestFiles(null))
    );

var host = builder.Build();
host.Run();
