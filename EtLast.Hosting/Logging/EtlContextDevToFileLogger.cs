namespace FizzCode.EtLast;

internal class EtlContextDevToFileLogger : IEtlContextLogger
{
    private readonly ILogger _logger;
    private readonly ILogger _ioLogger;
    private readonly string _directory;
    private readonly Lock _customFileLock = new();

    public EtlContextDevToFileLogger(IEtlContext context, string directory, LogSeverity minimumLogLevel, int retentionHours)
    {
        try
        {
            if (Directory.Exists(directory))
            {
                var existingDirectories = Directory.EnumerateDirectories(directory, "*", new EnumerationOptions()
                {
                    IgnoreInaccessible = true,
                    RecurseSubdirectories = false,
                    ReturnSpecialDirectories = false,
                });

                var now = DateTime.UtcNow;
                foreach (var dir in existingDirectories)
                {
                    var n = Path.GetFileName(dir);
                    if (DateTime.TryParseExact(n, EtlContext.ContextIdFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
                    {
                        if (date.AddHours(retentionHours) < now)
                        {
                            try
                            {
                                Directory.Delete(dir, recursive: true);
                            }
                            catch (Exception) { }
                        }
                    }
                }
            }
        }
        catch (Exception) { }

        _directory = Path.Combine(directory, context.Manifest.ContextId.ToString("D", CultureInfo.InvariantCulture));

        if (!Directory.Exists(_directory))
        {
            try
            {
                Directory.CreateDirectory(_directory);
            }
            catch (Exception) { }
        }

        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(_directory, "2-info.txt"),
                restrictedToMinimumLevel: LogEventLevel.Information,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(_directory, "3-warning.txt"),
                restrictedToMinimumLevel: LogEventLevel.Warning,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(_directory, "4-error.txt"),
                restrictedToMinimumLevel: LogEventLevel.Error,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8)

            .WriteTo.File(Path.Combine(_directory, "5-fatal.txt"),
                restrictedToMinimumLevel: LogEventLevel.Fatal,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                formatProvider: CultureInfo.InvariantCulture,
                encoding: Encoding.UTF8);

        if (minimumLogLevel <= LogSeverity.Debug)
        {
            config = config
                .WriteTo.File(Path.Combine(_directory, "1-debug.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(5),
                    encoding: Encoding.UTF8);
        }

        if (minimumLogLevel <= LogSeverity.Verbose)
        {
            config = config
                .WriteTo.File(Path.Combine(_directory, "0-verbose.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Verbose,
                    outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(5),
                    encoding: Encoding.UTF8);
        }

        config = config
            .MinimumLevel.Is(Debugger.IsAttached ? LogEventLevel.Verbose : LogEventLevel.Debug);

        _logger = config.CreateLogger();

        var ioConfig = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(_directory, "io.tsv"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz}\t{Message:l}{NewLine}",
                formatProvider: CultureInfo.InvariantCulture,
                buffered: true,
                flushToDiskInterval: TimeSpan.FromSeconds(5),
                encoding: Encoding.UTF8,
                hooks: new IoFileLifecycleHooks());

        _ioLogger = ioConfig.CreateLogger();
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

        var line = new StringBuilder()
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
        var msg = exception.FormatWithEtlDetails();
        Log(LogSeverity.Error, false, null, process, "{ErrorMessage}", msg);
    }

    public void OnContextIoCommandStart(IoCommand ioCommand)
    {
        if (_ioLogger == null)
            return;

        var sb = new StringBuilder();
        sb.Append(ioCommand.Process?.UniqueName);
        sb.Append('\t').Append(ioCommand.Id.ToString("D", CultureInfo.InvariantCulture));
        sb.Append('\t').Append(ioCommand.Kind.ToString());
        sb.Append('\t').Append("started");
        sb.Append('\t').Append(""); // affectedDataCount
        sb.Append('\t').Append(ioCommand.TimeoutSeconds != null ? ioCommand.TimeoutSeconds.Value.ToString("D", CultureInfo.InvariantCulture) : "");
        sb.Append('\t').Append(ioCommand.TransactionId);
        sb.Append('\t').Append(ioCommand.Location);
        sb.Append('\t').Append(ioCommand.Path);

        sb.Append('\t').Append(ioCommand.Message?.ReplaceLineEndings("\\n"));
        sb.Append('\t').Append(ioCommand.MessageExtra?.ReplaceLineEndings("\\n"));

        sb.Append('\t').Append(ioCommand.Command?.ReplaceLineEndings("\\n"));

        _ioLogger.Write(LogEventLevel.Information, sb.ToString());
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
    {
        if (_ioLogger == null)
            return;

        var sb = new StringBuilder();
        sb.Append(ioCommand.Process?.UniqueName);
        sb.Append('\t').Append(ioCommand.Id.ToString("D", CultureInfo.InvariantCulture));
        sb.Append('\t').Append(ioCommand.Kind.ToString());
        sb.Append('\t').Append(ioCommand.Exception != null ? "failed" : "succeeded");

        sb.Append('\t').Append(ioCommand.AffectedDataCount?.ToString("D", CultureInfo.InvariantCulture));

        sb.Append('\t').Append(""); // timeoutSeconds
        sb.Append('\t').Append(""); // transactionId
        sb.Append('\t').Append(""); // location
        sb.Append('\t').Append(""); // path

        if (ioCommand.Exception != null)
        {
            sb.Append('\t').Append("exception"); // message
            sb.Append(ioCommand.Exception.FormatWithEtlDetails().ReplaceLineEndings("\\n")); // messageExtra
        }

        _ioLogger.Write(LogEventLevel.Information, sb.ToString());
    }

    public void OnContextClosed()
    {
        try
        {
            (_logger as Logger)?.Dispose();
        }
        catch (Exception) { }
    }

    private class IoFileLifecycleHooks : Serilog.Sinks.File.FileLifecycleHooks
    {
        public override Stream OnFileOpened(string path, Stream underlyingStream, Encoding encoding)
        {
            if (underlyingStream.Length == 0)
            {
                using (var writer = new StreamWriter(underlyingStream, encoding, -1, true))
                {
                    var sb = new StringBuilder();
                    sb.Append("Timestamp");
                    sb.Append('\t').Append("ProcessUniqueName");
                    sb.Append('\t').Append("Id");
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

                    writer.WriteLine(sb.ToString());
                    writer.Flush();
                    underlyingStream.Flush();
                }
            }

            return base.OnFileOpened(path, underlyingStream, encoding);
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class EtlContextDevToFileLoggerFluent
{
    public static ISessionBuilder LogDevToFile(this ISessionBuilder builder, LogSeverity minimumLogLevel = LogSeverity.Debug, int retentionHours = 24 * 31)
    {
        builder.AddContextLogger(() => new EtlContextDevToFileLogger(builder.Context, builder.DevLogDirectory, minimumLogLevel, retentionHours));
        return builder;
    }
}