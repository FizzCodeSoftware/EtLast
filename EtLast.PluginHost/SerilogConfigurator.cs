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
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {Properties}{NewLine}{Exception}")

                .WriteTo.RollingFile(Path.Combine(logsFolder, "debug-{Date}.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {NewLine}{Exception}")

                .WriteTo.RollingFile(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "debug-{Date}.json"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit)

                .WriteTo.RollingFile(new CompactJsonFormatter(), Path.Combine(logsFolder, "debug-{Date}.structured.json"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit)

                .WriteTo.RollingFile(Path.Combine(logsFolder, "info-{Date}.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {NewLine}{Exception}")

                .WriteTo.RollingFile(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "info-{Date}.json"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit)

                .WriteTo.RollingFile(new CompactJsonFormatter(), Path.Combine(logsFolder, "info-{Date}.structured.json"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit)

                .WriteTo.RollingFile(Path.Combine(logsFolder, "warnings-{Date}.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {NewLine}{Exception}")

                .WriteTo.RollingFile(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "warnings-{Date}.json"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit)

                .WriteTo.RollingFile(Path.Combine(logsFolder, "errors-{Date}.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {NewLine}{Exception}")

                .WriteTo.RollingFile(new RenderedCompactJsonFormatter(), Path.Combine(logsFolder, "exceptions-{Date}.json"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit);

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

                .WriteTo.RollingFile(Path.Combine(logsFolder, "info-{Date}.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {NewLine}{Exception}")

                .WriteTo.RollingFile(Path.Combine(logsFolder, "warnings-{Date}.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {NewLine}{Exception}")

                .WriteTo.RollingFile(Path.Combine(logsFolder, "errors-{Date}.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    retainedFileCountLimit: configuration.RetainedLogFileCountLimit,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj} {NewLine}{Exception}");

            loggerConfig = loggerConfig.MinimumLevel.Is(LogEventLevel.Information);

            return loggerConfig.CreateLogger();
        }
    }
}