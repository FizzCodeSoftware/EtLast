using Serilog.Sinks.File;

namespace FizzCode.EtLast;

internal class EtlContextIoToFileLogger : IEtlContextListener
{
    private readonly ILogger _logger;

    public EtlContextIoToFileLogger(string folder, int lowFileCount = 4)
    {
        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(folder, "io-.tsv"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz}\t{Message:l}{NewLine}",
                formatProvider: CultureInfo.InvariantCulture,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: lowFileCount,
                buffered: true,
                flushToDiskInterval: TimeSpan.FromSeconds(1),
                hooks: new IoFileLifecycleHooks(),
                encoding: Encoding.UTF8);

        _logger = config.CreateLogger();
    }

    public void Start()
    {
    }

    public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
    }

    public void Log(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
    {
    }

    public void OnException(IProcess process, Exception exception)
    {
    }

    public void OnContextIoCommandStart(IoCommand ioCommand)
    {
        var sb = new StringBuilder();
        sb.Append(ioCommand.Process?.InvocationName);
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
        sb.Append('\t').Append(ioCommand.Process?.GetTopic()?.ReplaceLineEndings("\\n"));

        _logger.Write(LogEventLevel.Information, sb.ToString());
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
    {
        var sb = new StringBuilder();
        sb.Append(ioCommand.Process?.InvocationName);
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
            sb.Append(ioCommand.Exception.FormatExceptionWithDetails().ReplaceLineEndings("\\n")); // messageExtra
        }

        // topic

        _logger.Write(LogEventLevel.Information, sb.ToString());
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

[EditorBrowsable(EditorBrowsableState.Never)]
public static class EtlContextIoToFileLoggerFluent
{
    public static ISessionBuilder LogIoToFile(this ISessionBuilder builder, int lowFileCount = 4)
    {
        builder.Context.Listeners.Add(new EtlContextIoToFileLogger(builder.DevLogFolder, lowFileCount));
        return builder;
    }
}