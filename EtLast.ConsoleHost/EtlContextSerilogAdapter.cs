using Serilog.Sinks.File;

namespace FizzCode.EtLast.ConsoleHost;
internal class EtlContextSerilogAdapter : IEtlContextListener
{
    private readonly ILogger _logger;
    private readonly ILogger _opsLogger;
    private readonly ILogger _ioLogger;
    private readonly string _devLogFolder;
    private readonly string _opsLogFolder;
    private readonly object _customFileLock = new();

    public EtlContextSerilogAdapter(EnvironmentSettings environmentSettings, string devLogFolder, string opsLogFolder)
    {
        _logger = CreateLogger(environmentSettings, devLogFolder);
        _opsLogger = CreateOpsLogger(environmentSettings, opsLogFolder);
        _ioLogger = CreateIoLogger(environmentSettings, devLogFolder);

        _devLogFolder = devLogFolder;
        _opsLogFolder = opsLogFolder;
    }

    public void Start()
    {
    }

    private ILogger CreateLogger(EnvironmentSettings settings, string folder)
    {
        var config = new LoggerConfiguration();

        if (settings.FileLogSettings.Enabled)
        {
            config = config
                .WriteTo.File(new CompactJsonFormatter(), Path.Combine(folder, "events-.json"),
                    restrictedToMinimumLevel: (LogEventLevel)settings.FileLogSettings.MinimumLogLevel,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.InfoFileCount,
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(1),
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(folder, "2-info-.txt"),
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.InfoFileCount,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(folder, "3-warning-.txt"),
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(folder, "4-error-.txt"),
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(folder, "5-fatal-.txt"),
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    restrictedToMinimumLevel: LogEventLevel.Fatal,
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                    encoding: Encoding.UTF8);

            if (settings.FileLogSettings.MinimumLogLevel <= LogSeverity.Debug)
            {
                config = config
                    .WriteTo.File(Path.Combine(folder, "1-debug-.txt"),
                        restrictedToMinimumLevel: LogEventLevel.Debug,
                        outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                        formatProvider: CultureInfo.InvariantCulture,
                        rollingInterval: RollingInterval.Day,
                        retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
                        buffered: true,
                        flushToDiskInterval: TimeSpan.FromSeconds(1),
                        encoding: Encoding.UTF8);
            }

            if (settings.FileLogSettings.MinimumLogLevel <= LogSeverity.Verbose)
            {
                config = config
                    .WriteTo.File(Path.Combine(folder, "0-verbose-.txt"),
                        restrictedToMinimumLevel: LogEventLevel.Verbose,
                        outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                        formatProvider: CultureInfo.InvariantCulture,
                        rollingInterval: RollingInterval.Day,
                        retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
                        buffered: true,
                        flushToDiskInterval: TimeSpan.FromSeconds(1),
                        encoding: Encoding.UTF8);
            }
        }

        if (settings.ConsoleLogSettings.Enabled)
        {
            config = config
                .WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"),
                    (LogEventLevel)settings.ConsoleLogSettings.MinimumLogLevel);
        }

        config = config.MinimumLevel.Is(Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

        if (!string.IsNullOrEmpty(settings.SeqSettings.Url))
        {
            config = config
                .WriteTo.Seq(settings.SeqSettings.Url, apiKey: settings.SeqSettings.ApiKey);
        }

        return config.CreateLogger();
    }

    private ILogger CreateOpsLogger(EnvironmentSettings settings, string folder)
    {
        var config = new LoggerConfiguration();

        if (settings.FileLogSettings.Enabled)
        {
            config = config
                .WriteTo.File(Path.Combine(folder, "2-info-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.InfoFileCount,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(folder, "3-warning-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Warning,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(folder, "4-error-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Error,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                    encoding: Encoding.UTF8)

                .WriteTo.File(Path.Combine(folder, "5-fatal-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Fatal,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.ImportantFileCount,
                    encoding: Encoding.UTF8);
        }

        config = config.MinimumLevel.Is(LogEventLevel.Information);

        return config.CreateLogger();
    }

    private ILogger CreateIoLogger(EnvironmentSettings settings, string folder)
    {
        if (!settings.FileLogSettings.Enabled)
            return null;

        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(folder, "io-.tsv"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz}\t{Message:l}{NewLine}",
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
                buffered: true,
                flushToDiskInterval: TimeSpan.FromSeconds(1),
                hooks: new IoFileLifecycleHooks(),
                encoding: Encoding.UTF8);

        if (!string.IsNullOrEmpty(settings.SeqSettings.Url))
        {
            config = config
                .WriteTo.Seq(settings.SeqSettings.Url, apiKey: settings.SeqSettings.ApiKey);
        }

        return config.CreateLogger();
    }

    public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
        Log(severity, forOps, transactionId, process, text, args);
    }

    public void Log(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
        var sb = new StringBuilder();
        var values = new List<object>();

        if (process != null)
        {
            var proc = process.InvocationInfo.Caller;
            while (proc != null)
            {
                sb.Append("  ");
                proc = proc.InvocationInfo.Caller;
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

        var logger = forOps
            ? _opsLogger
            : _logger;

        logger.Write((LogEventLevel)severity, sb.ToString(), values.ToArray());
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
    {
        var logsFolder = forOps
           ? _opsLogFolder
           : _devLogFolder;

        if (!Directory.Exists(logsFolder))
        {
            try
            {
                Directory.CreateDirectory(logsFolder);
            }
            catch (Exception)
            {
                if (!Directory.Exists(logsFolder))
                    return;
            }
        }

        var filePath = Path.Combine(logsFolder, fileName);

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
        {
            File.AppendAllText(filePath, line + Environment.NewLine);
        }
    }

    public void OnException(IProcess process, Exception exception)
    {
        var opsErrors = new List<string>();
        GetOpsMessagesRecursive(exception, opsErrors);
        if (opsErrors.Count == 0)
            opsErrors.Add("error happened, please check the full log file for details");

        foreach (var opsError in opsErrors)
        {
            Log(LogSeverity.Error, true, null, process, opsError);
        }

        var msg = exception.FormatExceptionWithDetails();
        Log(LogSeverity.Error, false, null, process, "{ErrorMessage}", msg);
    }

    public void GetOpsMessagesRecursive(Exception ex, List<string> messages)
    {
        if (ex.Data.Contains(EtlException.OpsMessageDataKey))
        {
            var msg = ex.Data[EtlException.OpsMessageDataKey];
            if (msg != null)
            {
                messages.Add(msg.ToString());
            }
        }

        if (ex.InnerException != null)
            GetOpsMessagesRecursive(ex.InnerException, messages);

        if (ex is AggregateException aex)
        {
            foreach (var iex in aex.InnerExceptions)
            {
                GetOpsMessagesRecursive(iex, messages);
            }
        }
    }

    public void OnContextIoCommandStart(long uid, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra = null)
    {
        if (_ioLogger == null)
            return;

        var sb = new StringBuilder();
        sb.Append(process?.InvocationName);
        sb.Append('\t').Append(uid.ToString("D", CultureInfo.InvariantCulture));
        sb.Append('\t').Append(kind.ToString());
        sb.Append('\t').Append("started");
        sb.Append('\t').Append(""); // affectedDataCount
        sb.Append('\t').Append(timeoutSeconds != null ? timeoutSeconds.Value.ToString("D", CultureInfo.InvariantCulture) : "");
        sb.Append('\t').Append(transactionId);
        sb.Append('\t').Append(location);
        sb.Append('\t').Append(path);

        sb.Append('\t').Append(message?.ReplaceLineEndings("\\n"));
        sb.Append('\t').Append(messageExtra?.ReplaceLineEndings("\\n"));

        sb.Append('\t').Append(command?.ReplaceLineEndings("\\n"));
        sb.Append('\t').Append(process?.GetTopic()?.ReplaceLineEndings("\\n"));

        _ioLogger.Write(LogEventLevel.Information, sb.ToString());
    }

    public void OnContextIoCommandEnd(IProcess process, long uid, IoCommandKind kind, long? affectedDataCount, Exception ex)
    {
        if (_ioLogger == null)
            return;

        var sb = new StringBuilder();
        sb.Append(process?.InvocationName);
        sb.Append('\t').Append(uid.ToString("D", CultureInfo.InvariantCulture));
        sb.Append('\t').Append(kind.ToString());
        sb.Append('\t').Append(ex != null ? "failed" : "succeeded");

        sb.Append('\t').Append(affectedDataCount?.ToString("D", CultureInfo.InvariantCulture));

        sb.Append('\t').Append(""); // timeoutSeconds
        sb.Append('\t').Append(""); // transactionId
        sb.Append('\t').Append(""); // location
        sb.Append('\t').Append(""); // path

        if (ex != null)
        {
            sb.Append('\t').Append("exception"); // message
            sb.Append(ex.FormatExceptionWithDetails().ReplaceLineEndings("\\n")); // messageExtra
        }

        // topic

        _ioLogger.Write(LogEventLevel.Information, sb.ToString());
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

    public void OnSinkStarted(long sinkUid, string location, string path)
    {
    }

    public void OnWriteToSink(IReadOnlyRow row, long sinkUid)
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
            (_opsLogger as Logger)?.Dispose();
            (_ioLogger as Logger)?.Dispose();
        }
        catch (Exception) { }
    }

    private class IoFileLifecycleHooks : FileLifecycleHooks
    {
        public override Stream OnFileOpened(string path, Stream underlyingStream, Encoding encoding)
        {
            if (underlyingStream.Length == 0)
            {
                using (var writer = new StreamWriter(underlyingStream, encoding, -1, true))
                {
                    var sb = new StringBuilder();
                    sb.Append("Timestamp");
                    sb.Append('\t').Append("ProcessInvocationName");
                    sb.Append('\t').Append("UID");
                    sb.Append('\t').Append("Kind");
                    sb.Append('\t').Append("Action");
                    sb.Append('\t').Append("AffectedDataCount");
                    sb.Append('\t').Append("Timeout");
                    sb.Append('\t').Append("TransactionId");
                    sb.Append('\t').Append("Location");
                    sb.Append('\t').Append("Path");
                    sb.Append('\t').Append("Message");
                    sb.Append('\t').Append("Message Extra");
                    sb.Append('\t').Append("Command");
                    sb.Append('\t').Append("Topic");

                    writer.WriteLine(sb.ToString());
                    writer.Flush();
                    underlyingStream.Flush();
                }

            }

            return base.OnFileOpened(path, underlyingStream, encoding);
        }
    }
}