namespace FizzCode.EtLast.ConsoleHost;
internal class EtlSessionSerilogAdapter : IEtlContextListener
{
    private readonly ILogger _logger;
    private readonly ILogger _opsLogger;
    private readonly ILogger _ioLogger;
    private readonly string _devLogFolder;
    private readonly string _opsLogFolder;
    private readonly object _customFileLock = new();

    public EtlSessionSerilogAdapter(EnvironmentSettings environmentSettings, string devLogFolder, string opsLogFolder)
    {
        _logger = CreateLogger(environmentSettings, devLogFolder);
        _opsLogger = CreateOpsLogger(environmentSettings, opsLogFolder);
        _ioLogger = CreateIoLogger(environmentSettings, devLogFolder);

        _devLogFolder = devLogFolder;
        _opsLogFolder = opsLogFolder;
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
                    encoding: Encoding.UTF8)

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

            if (settings.FileLogSettings.MinimumLogLevel <= LogSeverity.Debug)
            {
                config = config
                    .WriteTo.File(Path.Combine(folder, "1-debug-.txt"),
                        restrictedToMinimumLevel: LogEventLevel.Debug,
                        outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                        formatProvider: CultureInfo.InvariantCulture,
                        rollingInterval: RollingInterval.Day,
                        retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
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
            config = config.WriteTo.Seq(settings.SeqSettings.Url, apiKey: settings.SeqSettings.ApiKey);
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
        var config = new LoggerConfiguration();

        if (settings.FileLogSettings.Enabled)
        {
            config = config
                .WriteTo.File(Path.Combine(folder, "io-.txt"),
                    restrictedToMinimumLevel: (LogEventLevel)settings.FileLogSettings.MinimumLogLevelIo,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: settings.FileLogSettings.RetainSettings.LowFileCount,
                    encoding: Encoding.UTF8);
        }

        if (settings.ConsoleLogSettings.Enabled)
        {
            config = config
                .WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"),
                    (LogEventLevel)settings.ConsoleLogSettings.MinimumLogLevel);
        }

        config = config.MinimumLevel.Is(LogEventLevel.Verbose);

        if (!string.IsNullOrEmpty(settings.SeqSettings.Url))
        {
            config = config.WriteTo.Seq(settings.SeqSettings.Url, apiKey: settings.SeqSettings.ApiKey);
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

    public void OnContextIoCommandStart(int uid, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
    {
        if (message != null)
        {
            var sb = new StringBuilder();
            var values = new List<object>();

            if (process != null)
            {
                var proc = process.InvocationInfo.Caller;
                while (proc != null)
                {
                    sb.Append('.');
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

            sb.Append("{IoCommandUid}/{IoCommandKind} started");
            values.Add("IO#" + uid.ToString("D", CultureInfo.InvariantCulture));
            values.Add(kind.ToString());

            if (location != null)
            {
                sb.Append(", location: {IoCommandTarget}");
                values.Add(location);
            }

            if (path != null)
            {
                sb.Append(", path: {IoCommandTargetPath}");
                values.Add(path);
            }

            if (timeoutSeconds != null)
            {
                sb.Append(", timeout: {IoCommandTimeout}");
                values.Add(timeoutSeconds.Value);
            }

            if (transactionId != null)
            {
                sb.Append(", transaction: {ActiveTransaction}");
                values.Add(transactionId);
            }

            sb.Append(", message: ").Append(message);
            if (messageArgs != null)
                values.AddRange(messageArgs);

            if (command != null)
            {
                sb.Append(", command: {IoCommand}");
                values.Add(command);
            }

            var topic = process?.GetTopic();
            if (topic != null)
            {
                sb.Append(" TPC#{ActiveTopic}");
                values.Add(topic);
            }

            _ioLogger.Write(LogEventLevel.Verbose, sb.ToString(), values.ToArray());
        }
    }

    public void OnContextIoCommandEnd(IProcess process, int uid, IoCommandKind kind, int? affectedDataCount, Exception ex)
    {
        var sb = new StringBuilder();
        var values = new List<object>();

        if (process != null)
        {
            var proc = process.InvocationInfo.Caller;
            while (proc != null)
            {
                sb.Append('.');
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

        sb.Append("{IoCommandUid}/{IoCommandKind} {IoResult}");
        values.Add("IO#" + uid.ToString("D", CultureInfo.InvariantCulture));
        values.Add(kind.ToString());
        values.Add(ex == null
            ? "finished"
            : "failed");

        if (ex == null)
        {
            if (affectedDataCount != null)
            {
                sb.Append(", affected data count: {AffectedDataCount}");
                values.Add(affectedDataCount);
            }
        }
        else
        {
            sb.Append(", {ErrorMessage}");
            values.Add(ex.FormatExceptionWithDetails());
        }

        _ioLogger.Write(ex == null
            ? LogEventLevel.Verbose
            : LogEventLevel.Error, sb.ToString(), values.ToArray());
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

    public void OnSinkStarted(int sinkUid, string location, string path)
    {
    }

    public void OnWriteToSink(IReadOnlyRow row, int sinkUid)
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
    }
}
