namespace FizzCode.EtLast.ConsoleHost;

using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using FizzCode.EtLast.ConsoleHost.SerilogSink;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Events;

public class CommandContext
{
    public string HostLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-host");
    public string DevLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");
    public string OpsLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");

    public ILogger Logger { get; private set; }
    public HostConfiguration HostConfiguration { get; private set; }

    public string LoadedConfigurationFileName { get; private set; }

    public bool Load()
    {
        Logger = CreateCommandContextLogger();

        LoadedConfigurationFileName = "host-configuration.json";
        LoadedConfigurationFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), LoadedConfigurationFileName);

        if (!File.Exists(LoadedConfigurationFileName))
        {
            Logger.Write(LogEventLevel.Fatal, "can't find the host configuration file: {FileName}", LoadedConfigurationFileName);
            return false;
        }

        HostConfiguration = new HostConfiguration();

        try
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile(LoadedConfigurationFileName, false)
                .Build();

            HostConfiguration.LoadFromConfiguration(configuration, "EtlHost");
        }
        catch (Exception ex)
        {
            throw new ConfigurationFileException(PathHelpers.GetFriendlyPathName(LoadedConfigurationFileName), "can't read the configuration file", ex);
        }

        if (HostConfiguration.ModulesFolder.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
        {
            HostConfiguration.ModulesFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), HostConfiguration.ModulesFolder[2..]);
        }

        if (!Directory.Exists(HostConfiguration.ModulesFolder))
        {
            Logger.Write(LogEventLevel.Fatal, "can't find the specified modules folder: {ModulesFolder}", HostConfiguration.ModulesFolder);
            return false;
        }

        return true;
    }

    public ILogger CreateCommandContextLogger()
    {
        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(HostLogFolder, "commands-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Debug,
                retainedFileCountLimit: int.MaxValue,
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8);

        config.WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"), LogEventLevel.Debug);

        config.MinimumLevel.Is(LogEventLevel.Debug);

        return config.CreateLogger();
    }
}
