using Serilog.Sinks.File;

namespace FizzCode.EtLast;

internal class EtlContextIoToFileLogger : IEtlContextListener
{
    private readonly ILogger _logger;

    public EtlContextIoToFileLogger(IEtlContext context, string directory, int lowFileCount = 4)
    {
        var config = new LoggerConfiguration()
            .WriteTo.File(Path.Combine(directory, "io-.tsv"),
                outputTemplate: "{Timestamp:HH:mm:ss.fff zzz}\t{ContextId}\t{Message:l}{NewLine}",
                formatProvider: CultureInfo.InvariantCulture,
                buffered: true,
                flushToDiskInterval: TimeSpan.FromSeconds(1),
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: lowFileCount,
                encoding: Encoding.UTF8,
                hooks: new IoFileLifecycleHooks())
            .Enrich.WithProperty("ContextId", context.Manifest.ContextId);

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

        _logger.Write(LogEventLevel.Information, sb.ToString());
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
    {
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
            sb.Append(ioCommand.Exception.FormatExceptionWithDetails().ReplaceLineEndings("\\n")); // messageExtra
        }

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
public static class EtlContextIoToFileLoggerFluent
{
    public static ISessionBuilder LogIoToFile(this ISessionBuilder builder, int lowFileCount = 4)
    {
        builder.Context.Listeners.Add(new EtlContextIoToFileLogger(builder.Context, builder.DevLogDirectory, lowFileCount));
        return builder;
    }
}