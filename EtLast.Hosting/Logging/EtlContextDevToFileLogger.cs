namespace FizzCode.EtLast;

internal class EtlContextDevToFileLogger : IEtlContextListener
{
    private readonly ILogger _logger;
    private readonly string _folder;
    private readonly object _customFileLock = new();

    public EtlContextDevToFileLogger(string folder, LogSeverity minimumLogLevel, int importantFileCount = 30, int infoFileCount = 14, int lowFileCount = 4)
    {
        _folder = folder;
        var config = new LoggerConfiguration()
            .WriteTo.File(new Serilog.Formatting.Compact.CompactJsonFormatter(), Path.Combine(folder, "events-.json"),
                restrictedToMinimumLevel: (LogEventLevel)minimumLogLevel,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: infoFileCount,
                buffered: true,
                flushToDiskInterval: TimeSpan.FromSeconds(1),
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(folder, "2-info-.txt"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                restrictedToMinimumLevel: LogEventLevel.Information,
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: infoFileCount,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(folder, "3-warning-.txt"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                restrictedToMinimumLevel: LogEventLevel.Warning,
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: importantFileCount,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(folder, "4-error-.txt"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                restrictedToMinimumLevel: LogEventLevel.Error,
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: importantFileCount,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(folder, "5-fatal-.txt"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                restrictedToMinimumLevel: LogEventLevel.Fatal,
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: importantFileCount,
                encoding: Encoding.UTF8);

        if (minimumLogLevel <= LogSeverity.Debug)
        {
            config = config
                .WriteTo.File(Path.Combine(folder, "1-debug-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: lowFileCount,
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(1),
                    encoding: Encoding.UTF8);
        }

        if (minimumLogLevel <= LogSeverity.Verbose)
        {
            config = config
                .WriteTo.File(Path.Combine(folder, "0-verbose-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Verbose,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: lowFileCount,
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(1),
                    encoding: Encoding.UTF8);
        }

        config = config.MinimumLevel.Is(Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

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
            var proc = process.InvocationInfo.Caller as IProcess;
            while (proc != null)
            {
                sb.Append("  ");
                proc = proc.InvocationInfo.Caller as IProcess;
            }

            if (process is IEtlTask)
            {
                sb.Append("{ActiveTask} ");
                values.Add(process.InvocationName);
            }
            else
            {
                sb.Append("{ActiveProcess} ");
                values.Add(process.InvocationName);
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

        _logger.Write((LogEventLevel)severity, sb.ToString(), values.ToArray());
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
    {
        if (forOps)
            return;

        if (!Directory.Exists(_folder))
        {
            try
            {
                Directory.CreateDirectory(_folder);
            }
            catch (Exception)
            {
                if (!Directory.Exists(_folder))
                    return;
            }
        }

        var filePath = Path.Combine(_folder, fileName);

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

    public void OnProcessInvocationStart(IProcess process)
    {
    }

    public void OnProcessInvocationEnd(IProcess process)
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
        builder.Context.Listeners.Add(new EtlContextDevToFileLogger(builder.DevLogFolder, minimumLogLevel, importantFileCount, infoFileCount, lowFileCount));
        return builder;
    }
}