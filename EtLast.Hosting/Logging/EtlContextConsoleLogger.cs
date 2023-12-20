namespace FizzCode.EtLast;

internal class EtlContextConsoleLogger : IEtlContextListener
{
    private readonly ILogger _logger;

    public EtlContextConsoleLogger(LogSeverity minimumLogLevel)
    {
        var config = new LoggerConfiguration()
            .WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"),
                (LogEventLevel)minimumLogLevel);

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
public static class EtlContextConsoleLoggerFluent
{
    public static ISessionBuilder LogToConsole(this ISessionBuilder builder, LogSeverity minimumLogLevel = LogSeverity.Information)
    {
        builder.Context.Listeners.Add(new EtlContextConsoleLogger(minimumLogLevel));
        return builder;
    }
}