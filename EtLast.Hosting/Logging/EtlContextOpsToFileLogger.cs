﻿namespace FizzCode.EtLast;

internal class EtlContextOpsToFileLogger : IEtlContextLogger
{
    private readonly ILogger _logger;
    private readonly string _directory;
    private readonly Lock _customFileLock = new();

    public EtlContextOpsToFileLogger(IEtlContext context, string directory, int importantFileCount = 30, int infoFileCount = 14)
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

        config = config
            .MinimumLevel.Is(LogEventLevel.Information)
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
        if (!forOps)
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
        if (!forOps)
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
        var opsErrors = new List<string>();
        GetOpsMessagesRecursive(exception, opsErrors);
        if (opsErrors.Count == 0)
            opsErrors.Add("error happened, please check the full log file for details");

        foreach (var opsError in opsErrors)
            Log(LogSeverity.Error, true, null, process, opsError);
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

    public void OnContextIoCommandStart(IoCommand ioCommand)
    {
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
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
public static class EtlContextOpsToFileLoggerFluent
{
    public static ISessionBuilder LogOpsToFile(this ISessionBuilder session, int importantFileCount = 30, int infoFileCount = 14)
    {
        session.AddContextLogger(() => new EtlContextOpsToFileLogger(session.Context, session.OpsLogDirectory, importantFileCount, infoFileCount));
        return session;
    }
}