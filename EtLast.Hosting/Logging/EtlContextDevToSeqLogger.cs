namespace FizzCode.EtLast;

internal class EtlContextDevToSeqLogger : IEtlContextLogger
{
    private readonly ILogger _logger;

    public EtlContextDevToSeqLogger(IEtlContext context, string url, string apiKey, LogSeverity minimumLogLevel)
    {
        var config = new LoggerConfiguration()
            .MinimumLevel.Is((LogEventLevel)minimumLogLevel)
            .WriteTo.Seq(url, apiKey: apiKey)
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

        _logger.Write((LogEventLevel)severity, sb.ToString(), [.. values]);
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
    {
    }

    public void OnException(IProcess process, Exception exception)
    {
        var msg = exception.FormatWithEtlDetails();
        Log(LogSeverity.Error, false, null, process, "{ErrorMessage}", msg);
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
public static class EtlContextDevToSeqLoggerFluent
{
    public static ISessionBuilder LogDevToSeq(this ISessionBuilder session, string url, string apiKey, LogSeverity minimumLogLevel = LogSeverity.Debug)
    {
        session.AddContextLogger(() => new EtlContextDevToSeqLogger(session.Context, url, apiKey, minimumLogLevel));
        return session;
    }
}