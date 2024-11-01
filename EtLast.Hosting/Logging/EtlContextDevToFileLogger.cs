namespace FizzCode.EtLast;

internal class EtlContextDevToFileLogger : IEtlContextListener
{
    private readonly ILogger _logger;
    private readonly string _directory;
    private readonly object _customFileLock = new();

    public EtlContextDevToFileLogger(IEtlContext context, string directory, LogSeverity minimumLogLevel, int importantFileCount = 30, int infoFileCount = 14, int lowFileCount = 4)
    {
        _directory = directory;
        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(directory, "2-info-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Information,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{ContextId}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: infoFileCount,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(directory, "3-warning-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Warning,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{ContextId}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: importantFileCount,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(directory, "4-error-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Error,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{ContextId}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: importantFileCount,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(directory, "5-fatal-.txt"),
                restrictedToMinimumLevel: LogEventLevel.Fatal,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{ContextId}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: importantFileCount,
                encoding: Encoding.UTF8);

        if (minimumLogLevel <= LogSeverity.Debug)
        {
            config = config
                .WriteTo.File(Path.Combine(directory, "1-debug-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{ContextId}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(1),
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: lowFileCount,
                    encoding: Encoding.UTF8);
        }

        if (minimumLogLevel <= LogSeverity.Verbose)
        {
            config = config
                .WriteTo.File(Path.Combine(directory, "0-verbose-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Verbose,
                    outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{ContextId}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(1),
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: lowFileCount,
                    encoding: Encoding.UTF8);
        }

        config = config
            .MinimumLevel.Is(Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug)
            .Enrich.WithProperty("ContextId", context.Manifest.ContextId);

        _logger = config.CreateLogger();
    }

    public void Start()
    {
    }

    public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
        Log(severity, forOps, transactionId, process, text, args);
    }

    public void Log(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
        if (forOps)
            return;

        var sb = new StringBuilder();
        var values = new List<object>();

        if (process != null)
        {
            var proc = process.ExecutionInfo.Caller as IProcess;
            while (proc != null)
            {
                sb.Append("  ");
                proc = proc.ExecutionInfo.Caller as IProcess;
            }

            if (process is IEtlTask)
            {
                sb.Append("{ActiveTask} ");
                values.Add(process.UniqueName);
            }
            else
            {
                sb.Append("{ActiveProcess} ");
                values.Add(process.UniqueName);
            }
        }

        if (transactionId != null)
        {
            sb.Append("/{ActiveTransaction}/ ");
            values.Add(transactionId);
        }

        sb.Append(text);
        if (args != null)
            values.AddRange(args);

        var topic = process?.GetTopic();
        if (topic != null)
        {
            sb.Append(" TPC#{ActiveTopic}");
            values.Add(topic);
        }

        _logger.Write((LogEventLevel)severity, sb.ToString(), [.. values]);
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
    {
        if (forOps)
            return;

        if (!Directory.Exists(_directory))
        {
            try
            {
                Directory.CreateDirectory(_directory);
            }
            catch (Exception)
            {
                if (!Directory.Exists(_directory))
                    return;
            }
        }

        var filePath = Path.Combine(_directory, fileName);

        var topic = process?.GetTopic();

        var line = new StringBuilder()
            .Append(!string.IsNullOrEmpty(topic)
                ? topic + "\t"
                : "")
            .Append(process != null
                ? process.Name + "\t"
                : "")
            .AppendFormat(CultureInfo.InvariantCulture, text, args)
            .ToString();

        lock (_customFileLock)
            File.AppendAllText(filePath, line + Environment.NewLine);
    }

    public void OnException(IProcess process, Exception exception)
    {
        var msg = exception.FormatExceptionWithDetails();
        Log(LogSeverity.Error, false, null, process, "{ErrorMessage}", msg);
    }

    public void OnContextIoCommandStart(IoCommand ioCommand)
    {
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
    {
    }

    public void OnRowCreated(IReadOnlyRow row)
    {
    }

    public void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess)
    {
    }

    public void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values)
    {
    }

    public void OnSinkStarted(IProcess process, Sink sink)
    {
    }

    public void OnWriteToSink(Sink sink, IReadOnlyRow row)
    {
    }

    public void OnProcessStart(IProcess process)
    {
    }

    public void OnProcessEnd(IProcess process)
    {
    }

    public void OnContextClosed()
    {
        try
        {
            (_logger as Logger)?.Dispose();
        }
        catch (Exception) { }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class EtlContextDevToFileLoggerFluent
{
    public static ISessionBuilder LogDevToFile(this ISessionBuilder builder, LogSeverity minimumLogLevel = LogSeverity.Debug, int importantFileCount = 30, int infoFileCount = 14, int lowFileCount = 4)
    {
        builder.Context.Listeners.Add(new EtlContextDevToFileLogger(builder.Context, builder.DevLogDirectory, minimumLogLevel, importantFileCount, infoFileCount, lowFileCount));
        return builder;
    }
}