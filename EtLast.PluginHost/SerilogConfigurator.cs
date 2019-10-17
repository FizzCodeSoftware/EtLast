namespace FizzCode.EtLast.PluginHost
{
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using System.Text;
    using FizzCode.EtLast.PluginHost.SerilogSink;
    using Serilog;
    using Serilog.Events;
    using Serilog.Exceptions;
    using Serilog.Formatting.Compact;

    internal static class SerilogConfigurator
    {
        public static ILogger CreateLogger(HostConfiguration hostConfiguration)
        {
            var logsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");

            var config = new LoggerConfiguration()
                .Enrich.WithExceptionDetails()

                .WriteTo.File(new CompactJsonFormatter(), Path.Combine(logsFolder, "events-.json"),
                    restrictedToMinimumLevel: hostConfiguration?.MinimumLogLevelInFile ?? LogEventLevel.Debug,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitInfo ?? int.MaxValue,
                    rollingInterval: RollingInterval.Day,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(logsFolder, "2-info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitInfo ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(logsFolder, "3-warning-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(logsFolder, "4-error-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(logsFolder, "5-fatal-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Fatal,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitImportant ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);

            if ((hostConfiguration?.MinimumLogLevelInFile ?? LogEventLevel.Debug) <= LogEventLevel.Debug)
            {
                config.WriteTo.File(Path.Combine(logsFolder, "1-debug-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitLow ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);
            }

            if ((hostConfiguration?.MinimumLogLevelInFile ?? LogEventLevel.Debug) <= LogEventLevel.Verbose)
            {
                config.WriteTo.File(Path.Combine(logsFolder, "0-verbose-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Verbose,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimitLow ?? int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);
            }

            config.WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"), hostConfiguration?.MinimumLogLevelOnConsole ?? LogEventLevel.Debug);

            config = config.MinimumLevel.Is(System.Diagnostics.Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

            if (System.Diagnostics.Debugger.IsAttached)
            {
                config = config.Enrich.WithThreadId();
            }

            if (hostConfiguration != null && !string.IsNullOrEmpty(hostConfiguration.SeqUrl) && hostConfiguration.SeqUrl != "-")
            {
                config = config.WriteTo.Seq(hostConfiguration.SeqUrl, apiKey: hostConfiguration.SeqApiKey);
            }

            return config.CreateLogger();
        }

        public static ILogger CreateOpsLogger(HostConfiguration configuration)
        {
            var logsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");

            var loggerConfig = new LoggerConfiguration()
                .WriteTo.File(Path.Combine(logsFolder, "2-info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimitInfo,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(logsFolder, "3-warning-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimitImportant,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(logsFolder, "4-error-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimitImportant,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(logsFolder, "5-fatal-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Fatal,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimitImportant,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8);

            loggerConfig = loggerConfig.MinimumLevel.Is(LogEventLevel.Information);

            return loggerConfig.CreateLogger();
        }
    }
}