namespace FizzCode.EtLast.PluginHost
{
    using System.IO;
    using System.Reflection;
    using Serilog;
    using Serilog.Events;
    using Serilog.Exceptions;
    using Serilog.Formatting.Compact;
    using Serilog.Sinks.SystemConsole.Themes;

    internal class SerilogConfigurator
    {
        public ILogger CreateLogger(PluginHostConfiguration configuration)
        {
            var logsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");

            var loggerConfig = new LoggerConfiguration()
                .Enrich.WithThreadId()
                .Enrich.WithExceptionDetails()

                .WriteTo.Console(
                    restrictedToMinimumLevel: configuration.MinimumLogLevelOnConsole,
                    theme: AnsiConsoleTheme.Literate,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {Properties}{NewLine}{Exception}")

                .WriteTo.File(Path.Combine(logsFolder, "debug-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "debug-.json"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(new CompactJsonFormatter(), Path.Combine(logsFolder, "debug-.structured.json"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "info-.json"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(new CompactJsonFormatter(), Path.Combine(logsFolder, "info-.structured.json"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "warnings-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "warnings-.json"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(Path.Combine(logsFolder, "errors-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u4}] {Message:lj} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day)

                .WriteTo.File(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "exceptions-.json"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    rollingInterval: RollingInterval.Day);

            loggerConfig = loggerConfig.MinimumLevel.Is(System.Diagnostics.Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

            if (!string.IsNullOrEmpty(configuration.SeqUrl) && configuration.SeqUrl != "-")
            {
                loggerConfig = loggerConfig.WriteTo.Seq(configuration.SeqUrl, apiKey: configuration.SeqApiKey);
            }

            return loggerConfig.CreateLogger();
        }

        public ILogger CreateOpsLogger(PluginHostConfiguration configuration)
        {
            var logsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");

            var loggerConfig = new LoggerConfiguration()
                .Enrich.WithThreadId()
                .Enrich.WithExceptionDetails()

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