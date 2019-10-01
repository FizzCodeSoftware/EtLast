namespace FizzCode.EtLast.PluginHost
{
    using System.IO;
    using System.Reflection;
    using FizzCode.EtLast.PluginHost.SerilogSink;
    using Serilog;
    using Serilog.Events;
    using Serilog.Exceptions;
    using Serilog.Formatting.Compact;

    internal static class SerilogConfigurator
    {
        public static ILogger CreateLogger(PluginHostConfiguration hostConfiguration)
        {
            var logsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");

            var loggerConfig = new LoggerConfiguration()
                .Enrich.WithExceptionDetails()

                .WriteTo.File(new CompactJsonFormatter(), Path.Combine(logsFolder, "debug-.json"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimit ?? int.MaxValue,
                    rollingInterval: RollingInterval.Day)

                    .WriteTo.File(Path.Combine(logsFolder, "debug-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimit ?? int.MaxValue,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimit ?? int.MaxValue,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "warnings-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimit ?? int.MaxValue,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "errors-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: hostConfiguration?.RetainedLogFileCountLimit ?? int.MaxValue,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day);

            loggerConfig.WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message:lj} {Properties}{NewLine}{Exception}"), hostConfiguration?.MinimumLogLevelOnConsole ?? LogEventLevel.Debug);

            loggerConfig = loggerConfig.MinimumLevel.Is(System.Diagnostics.Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

            if (System.Diagnostics.Debugger.IsAttached)
            {
                loggerConfig = loggerConfig.Enrich.WithThreadId();
            }

            if (hostConfiguration != null && !string.IsNullOrEmpty(hostConfiguration.SeqUrl) && hostConfiguration.SeqUrl != "-")
            {
                loggerConfig = loggerConfig.WriteTo.Seq(hostConfiguration.SeqUrl, apiKey: hostConfiguration.SeqApiKey);
            }

            return loggerConfig.CreateLogger();
        }

        public static ILogger CreateOpsLogger(PluginHostConfiguration configuration)
        {
            var logsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");

            var loggerConfig = new LoggerConfiguration()
                .WriteTo.File(Path.Combine(logsFolder, "info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "warnings-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "errors-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day);

            loggerConfig = loggerConfig.MinimumLevel.Is(LogEventLevel.Information);

            return loggerConfig.CreateLogger();
        }
    }
}