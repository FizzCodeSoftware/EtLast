namespace FizzCode.EtLast.PluginHost
{
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using System.Text;
    using FizzCode.EtLast.PluginHost.SerilogSink;
    using Serilog;
    using Serilog.Events;
    using Serilog.Formatting.Compact;

    internal static class SerilogConfigurator
    {
        public static string DevLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");
        public static string OpsLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");

        public static ILogger CreateLogger(HostConfiguration hostConfiguration)
        {
            var config = new LoggerConfiguration()

                .WriteTo.File(new CompactJsonFormatter(), Path.Combine(DevLogFolder, "events-.json"),
                    restrictedToMinimumLevel: hostConfiguration?.MinimumLogLevelInFile ?? LogEventLevel.Debug,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitInfo ?? int.MaxValue,
                    rollingInterval: RollingInterval.Day,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(DevLogFolder, "2-info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitInfo ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(DevLogFolder, "3-warning-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(DevLogFolder, "4-error-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(DevLogFolder, "5-fatal-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Fatal,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);

            if ((hostConfiguration?.MinimumLogLevelInFile ?? LogEventLevel.Debug) <= LogEventLevel.Debug)
            {
                config.WriteTo.File(Path.Combine(DevLogFolder, "1-debug-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitLow ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);
            }

            if ((hostConfiguration?.MinimumLogLevelInFile ?? LogEventLevel.Debug) <= LogEventLevel.Verbose)
            {
                config.WriteTo.File(Path.Combine(DevLogFolder, "0-verbose-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Verbose,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitLow ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);
            }

            config.WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"), hostConfiguration?.MinimumLogLevelOnConsole ?? LogEventLevel.Debug);

            config = config.MinimumLevel.Is(System.Diagnostics.Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

            if (hostConfiguration != null && !string.IsNullOrEmpty(hostConfiguration.SeqUrl) && hostConfiguration.SeqUrl != "-")
            {
                config = config.WriteTo.Seq(hostConfiguration.SeqUrl, apiKey: hostConfiguration.SeqApiKey);
            }

            return config.CreateLogger();
        }

        public static ILogger CreateOpsLogger(HostConfiguration hostConfiguration)
        {
            var loggerConfig = new LoggerConfiguration()
                .WriteTo.File(Path.Combine(OpsLogFolder, "2-info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitInfo ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(OpsLogFolder, "3-warning-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(OpsLogFolder, "4-error-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(OpsLogFolder, "5-fatal-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Fatal,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);

            loggerConfig = loggerConfig.MinimumLevel.Is(LogEventLevel.Information);

            return loggerConfig.CreateLogger();
        }
    }
}