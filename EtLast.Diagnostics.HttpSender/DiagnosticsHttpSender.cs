using System.Globalization;
using System.Web;

namespace FizzCode.EtLast;

public class DiagnosticsHttpSender : IDisposable, IEtlContextListener
{
    /// <summary>
    /// Default value is 2.
    /// </summary>
    public int MaxCommunicationErrorCount { get; set; } = 2;

    /// <summary>
    /// Default value is "http://localhost:8642"
    /// </summary>
    public string Url { get; set; }

    private Uri _uri;
    private HttpClient _client;
    private Thread _workerThread;
    private readonly IEtlContext _context;
    private ExtendedBinaryWriter _currentWriter;
    private readonly ExtendedBinaryWriter _eventWriter = new(new MemoryStream(), Encoding.UTF8);
    private readonly object _currentWriterLock = new();
    private bool _finished;
    private int _communicationErrorCount;

    private readonly Dictionary<string, MessageTemplate> _messageTemplateCache = [];
    private readonly object _messageTemplateCacheLock = new();
    private readonly MessageTemplateParser _messageTemplateParser = new();

    public DiagnosticsHttpSender(IEtlContext context)
    {
        _context = context;

        _client = new HttpClient
        {
            Timeout = TimeSpan.FromMilliseconds(10000),
        };
    }

    public void Start()
    {
        if (Url == null)
            return;

        _uri = new Uri(Url);

        SendWriter(null);

        _workerThread = new Thread(WorkerMethod);
        _workerThread.Start();
    }

    private void WorkerMethod()
    {
        var swLastSent = Stopwatch.StartNew();

        while (!_finished && (_communicationErrorCount <= MaxCommunicationErrorCount))
        {
            ExtendedBinaryWriter writerToSend = null;
            lock (_currentWriterLock)
            {
                if (_currentWriter != null
                    && (_currentWriter.BaseStream.Length >= 1024 * 1024
                        || (_currentWriter.BaseStream.Length > 0 && swLastSent.ElapsedMilliseconds > 500)))
                {
                    writerToSend = _currentWriter;
                    _currentWriter = null;
                }
            }

            if (writerToSend != null)
                SendWriter(writerToSend);

            Thread.Sleep(10);
        }

        if (_currentWriter != null)
        {
            if (_currentWriter.BaseStream.Length > 0 && _communicationErrorCount <= MaxCommunicationErrorCount)
                SendWriter(_currentWriter);

            _currentWriter = null;
        }
    }

    private void SendWriter(ExtendedBinaryWriter writer)
    {
        writer?.Flush();

        var fullUri = new Uri(_uri, "diag?sid=" + _context.Manifest.ContextId.ToString("D", CultureInfo.InvariantCulture) + (_context.Manifest.ContextName != null ? "&ctx=" + HttpUtility.UrlEncode(_context.Manifest.ContextName) : ""));

        var binaryContent = writer != null
            ? (writer.BaseStream as MemoryStream).ToArray()
            : [];

        using (var content = new ByteArrayContent(binaryContent))
        {
            try
            {
                var response = _client.PostAsync(fullUri, content).Result;
                var responseBody = response.Content.ReadAsStringAsync().Result;
                if (responseBody != "ACK")
                {
                    _communicationErrorCount++;
                }
            }
            catch (Exception)
            {
                _communicationErrorCount++;
            }
        }

        writer?.BaseStream.Dispose();
        writer?.Dispose();
    }

    public void SendDiagnostics(DiagnosticsEventKind kind, Action<ExtendedBinaryWriter> writerDelegate)
    {
        if (_communicationErrorCount > MaxCommunicationErrorCount)
            return;

        lock (_currentWriterLock)
        {
            if (_finished)
                throw new Exception("unexpected call of " + nameof(SendDiagnostics));

            _currentWriter ??= new ExtendedBinaryWriter(new MemoryStream(), Encoding.UTF8);

            writerDelegate?.Invoke(_eventWriter);
            _eventWriter.Flush();

            var data = (_eventWriter.BaseStream as MemoryStream).ToArray();
            _eventWriter.BaseStream.Position = 0;
            _eventWriter.BaseStream.SetLength(0);

            //Debug.WriteLine(_currentWriter.BaseStream.Position + "\t" + kind + "\t" + data.Length);

            _currentWriter.Write((byte)kind);
            _currentWriter.Write7BitEncodedInt(data.Length);
            _currentWriter.Write(DateTime.Now.Ticks);
            _currentWriter.Write(data, 0, data.Length);
        }
    }

    private bool _isDisposed;

    protected virtual void Dispose(bool disposing)
    {
        if (!_isDisposed)
        {
            if (disposing)
            {
                _client?.Dispose();
                _client = null;

                _currentWriter?.Dispose();
                _currentWriter = null;
            }

            _isDisposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void Flush()
    {
        _finished = true;
        _workerThread.Join();
    }

    public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
    {
        if ((severity >= LogSeverity.Debug) && !string.IsNullOrEmpty(text) && !forOps)
        {
            if (args.Length == 0)
            {
                SendDiagnostics(DiagnosticsEventKind.Log, writer =>
                {
                    writer.WriteNullable(transactionId);
                    writer.Write(text);
                    writer.Write((byte)severity);
                    writer.WriteNullable7BitEncodedInt64(process?.ExecutionInfo?.Id);
                    writer.Write7BitEncodedInt(0);
                });

                return;
            }

            var template = GetMessageTemplate(text);
            var tokens = template.Tokens.ToList();

            SendDiagnostics(DiagnosticsEventKind.Log, writer =>
            {
                writer.WriteNullable(transactionId);
                writer.Write(text);
                writer.Write((byte)severity);
                writer.WriteNullable7BitEncodedInt64(process?.ExecutionInfo?.Id);

                var argCount = 0;
                for (var i = 0; i < tokens.Count && argCount < args.Length; i++)
                {
                    if (tokens[i] is PropertyToken pt)
                        argCount++;
                }

                writer.Write7BitEncodedInt(argCount);
                for (int i = 0, idx = 0; i < tokens.Count && idx < args.Length; i++)
                {
                    if (tokens[i] is PropertyToken pt)
                    {
                        var rawText = pt.ToString();
                        writer.Write(rawText);
                        writer.WriteObject(args[idx]);
                        idx++;
                    }
                }
            });
        }
    }

    public MessageTemplate GetMessageTemplate(string text)
    {
        if (string.IsNullOrEmpty(text))
            return null;

        lock (_messageTemplateCacheLock)
        {
            if (_messageTemplateCache.TryGetValue(text, out var existingTemplate))
                return existingTemplate;
        }

        var template = _messageTemplateParser.Parse(text);
        lock (_messageTemplateCacheLock)
        {
            if (!_messageTemplateCache.ContainsKey(text))
            {
                if (_messageTemplateCache.Count == 1000)
                    _messageTemplateCache.Clear();

                _messageTemplateCache[text] = template;
            }
        }

        return template;
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
    {
    }

    public void OnException(IProcess process, Exception exception)
    {
    }

    public void OnRowCreated(IReadOnlyRow row)
    {
        SendDiagnostics(DiagnosticsEventKind.RowCreated, writer =>
        {
            writer.Write7BitEncodedInt64(row.Owner.ExecutionInfo.Id);
            writer.Write7BitEncodedInt64(row.Id);
            writer.Write7BitEncodedInt(row.ValueCount);
            foreach (var kvp in row.Values)
            {
                writer.Write(kvp.Key);
                writer.WriteObject(kvp.Value);
            }
        });
    }

    public void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess)
    {
        SendDiagnostics(DiagnosticsEventKind.RowOwnerChanged, writer =>
        {
            writer.Write7BitEncodedInt64(row.Id);
            writer.Write7BitEncodedInt64(previousProcess.ExecutionInfo.Id);
            writer.WriteNullable7BitEncodedInt64(currentProcess?.ExecutionInfo?.Id);
        });
    }

    public void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values)
    {
        SendDiagnostics(DiagnosticsEventKind.RowValueChanged, writer =>
        {
            writer.Write7BitEncodedInt64(row.Id);
            writer.WriteNullable7BitEncodedInt64(row.Owner?.ExecutionInfo?.Id);

            writer.Write7BitEncodedInt(values.Length);
            foreach (var kvp in values)
            {
                writer.Write(kvp.Key);
                writer.WriteObject(kvp.Value);
            }
        });
    }

    public void OnSinkStarted(IProcess process, Sink sink)
    {
        SendDiagnostics(DiagnosticsEventKind.SinkStarted, writer =>
        {
            writer.Write7BitEncodedInt64(sink.Id);
            writer.WriteNullable(sink.Location);
            writer.WriteNullable(sink.Path);
            writer.WriteNullable(sink.Format);
            writer.Write7BitEncodedInt64(process.ExecutionInfo.Id);
        });
    }

    public void OnWriteToSink(Sink sink, IReadOnlyRow row)
    {
        SendDiagnostics(DiagnosticsEventKind.WriteToSink, writer =>
        {
            writer.Write7BitEncodedInt64(row.Id);
            writer.Write7BitEncodedInt64(row.Owner.ExecutionInfo.Id);
            writer.Write7BitEncodedInt64(sink.Id);
            writer.Write7BitEncodedInt(row.ValueCount);
            foreach (var kvp in row.Values)
            {
                writer.Write(kvp.Key);
                writer.WriteObject(kvp.Value);
            }
        });
    }

    public void OnProcessStart(IProcess process)
    {
        SendDiagnostics(DiagnosticsEventKind.ProcessStart, writer =>
        {
            writer.Write7BitEncodedInt64(process.ExecutionInfo.Id);
            writer.Write(process.GetType().GetFriendlyTypeName());
            writer.WriteNullable(process.Kind);
            writer.Write(process.Name);
            writer.WriteNullable(process.GetTopic());
            writer.WriteNullable7BitEncodedInt64((process.ExecutionInfo.Caller as IProcess)?.ExecutionInfo?.Id);
        });
    }

    public void OnProcessEnd(IProcess process)
    {
        SendDiagnostics(DiagnosticsEventKind.ProcessEnd, writer =>
        {
            writer.Write7BitEncodedInt64(process.ExecutionInfo.Id);
            writer.Write7BitEncodedInt64(process.ExecutionInfo.Timer.ElapsedMilliseconds);
            writer.WriteNullable7BitEncodedInt64(process.ExecutionInfo.NetTimeMilliseconds);
        });
    }

    public void OnContextIoCommandStart(IoCommand ioCommand)
    {
        SendDiagnostics(DiagnosticsEventKind.IoCommandStart, writer =>
        {
            writer.Write7BitEncodedInt64(ioCommand.Id);
            writer.Write7BitEncodedInt64(ioCommand.Process.ExecutionInfo.Id);
            writer.Write((byte)ioCommand.Kind);
            writer.WriteNullable(ioCommand.Location);
            writer.WriteNullable(ioCommand.Path);
            writer.WriteNullable7BitEncodedInt32(ioCommand.TimeoutSeconds);
            writer.WriteNullable(ioCommand.Command);
            writer.WriteNullable(ioCommand.TransactionId);
            var arguments = ioCommand.ArgumentListGetter?.Invoke()?.ToArray();
            if (arguments?.Length > 0)
            {
                writer.Write7BitEncodedInt(arguments.Length);
                foreach (var kvp in arguments)
                {
                    writer.Write(kvp.Key);
                    writer.WriteObject(kvp.Value);
                }
            }
            else
            {
                writer.Write7BitEncodedInt(0);
            }
        });
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
    {
        SendDiagnostics(DiagnosticsEventKind.IoCommandEnd, writer =>
        {
            writer.Write7BitEncodedInt64(ioCommand.Id);
            writer.WriteNullable7BitEncodedInt64(ioCommand.AffectedDataCount);
            writer.WriteNullable(ioCommand.Exception?.FormatExceptionWithDetails());
        });
    }

    public void OnContextClosed()
    {
        SendDiagnostics(DiagnosticsEventKind.ContextEnded, null);

        Flush();
    }
}