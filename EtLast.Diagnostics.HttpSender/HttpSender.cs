using System.Web;

namespace FizzCode.EtLast.Diagnostics;

public class HttpSender : IDisposable, IEtlContextListener
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
    private readonly string _contextId;
    private readonly string _contextName;
    private ExtendedBinaryWriter _currentWriter;
    private readonly ExtendedBinaryWriter _eventWriter = new(new MemoryStream(), Encoding.UTF8);
    private readonly ExtendedBinaryWriter _dictWriter = new(new MemoryStream(), Encoding.UTF8);
    private readonly object _currentWriterLock = new();
    private bool _finished;
    private int _communicationErrorCount;

    private readonly Dictionary<string, MessageTemplate> _messageTemplateCache = new();
    private readonly object _messageTemplateCacheLock = new();
    private readonly MessageTemplateParser _messageTemplateParser = new();

    public HttpSender(IEtlContext context)
    {
        _contextId = context.Id;
        _contextName = context.Name;

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

        var fullUri = new Uri(_uri, "diag?sid=" + _contextId + (_contextName != null ? "&ctx=" + HttpUtility.UrlEncode(_contextName) : ""));

        var binaryContent = writer != null
            ? (writer.BaseStream as MemoryStream).ToArray()
            : Array.Empty<byte>();

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
                    writer.WriteNullable7BitEncodedInt64(process?.InvocationInfo?.InvocationUid);
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
                writer.WriteNullable7BitEncodedInt64(process?.InvocationInfo?.InvocationUid);

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
            writer.Write7BitEncodedInt64(row.CurrentProcess.InvocationInfo.InvocationUid);
            writer.Write7BitEncodedInt64(row.Uid);
            writer.Write7BitEncodedInt(row.ColumnCount);
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
            writer.Write7BitEncodedInt64(row.Uid);
            writer.Write7BitEncodedInt64(previousProcess.InvocationInfo.InvocationUid);
            writer.WriteNullable7BitEncodedInt64(currentProcess?.InvocationInfo?.InvocationUid);
        });
    }

    public void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values)
    {
        SendDiagnostics(DiagnosticsEventKind.RowValueChanged, writer =>
        {
            writer.Write7BitEncodedInt64(row.Uid);
            writer.WriteNullable7BitEncodedInt64(row.CurrentProcess?.InvocationInfo?.InvocationUid);

            writer.Write7BitEncodedInt(values.Length);
            foreach (var kvp in values)
            {
                writer.Write(kvp.Key);
                writer.WriteObject(kvp.Value);
            }
        });
    }

    public void OnSinkStarted(long sinkUid, string location, string path)
    {
        SendDiagnostics(DiagnosticsEventKind.SinkStarted, writer =>
        {
            writer.Write7BitEncodedInt64(sinkUid);
            writer.WriteNullable(location);
            writer.WriteNullable(path);
        });
    }

    public void OnWriteToSink(IReadOnlyRow row, long sinkUid)
    {
        SendDiagnostics(DiagnosticsEventKind.WriteToSink, writer =>
        {
            writer.Write7BitEncodedInt64(row.Uid);
            writer.Write7BitEncodedInt64(row.CurrentProcess.InvocationInfo.InvocationUid);
            writer.Write7BitEncodedInt64(sinkUid);
            writer.Write7BitEncodedInt(row.ColumnCount);
            foreach (var kvp in row.Values)
            {
                writer.Write(kvp.Key);
                writer.WriteObject(kvp.Value);
            }
        });
    }

    public void OnProcessInvocationStart(IProcess process)
    {
        SendDiagnostics(DiagnosticsEventKind.ProcessInvocationStart, writer =>
        {
            writer.Write7BitEncodedInt64(process.InvocationInfo.InvocationUid);
            writer.Write7BitEncodedInt64(process.InvocationInfo.InstanceUid);
            writer.Write7BitEncodedInt64(process.InvocationInfo.Number);
            writer.Write(process.GetType().GetFriendlyTypeName());
            writer.WriteNullable(process.Kind);
            writer.Write(process.Name);
            writer.WriteNullable(process.GetTopic());
            writer.WriteNullable7BitEncodedInt64(process.InvocationInfo.Caller?.InvocationInfo?.InvocationUid);
        });
    }

    public void OnProcessInvocationEnd(IProcess process)
    {
        SendDiagnostics(DiagnosticsEventKind.ProcessInvocationEnd, writer =>
        {
            writer.Write7BitEncodedInt64(process.InvocationInfo.InvocationUid);
            writer.Write7BitEncodedInt64(process.InvocationInfo.InvocationStarted.ElapsedMilliseconds);
            writer.WriteNullable7BitEncodedInt64(process.InvocationInfo.LastInvocationNetTimeMilliseconds);
        });
    }

    public void OnContextIoCommandStart(long uid, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra)
    {
        SendDiagnostics(DiagnosticsEventKind.IoCommandStart, writer =>
        {
            writer.Write7BitEncodedInt64(uid);
            writer.Write7BitEncodedInt64(process.InvocationInfo.InvocationUid);
            writer.Write((byte)kind);
            writer.WriteNullable(location);
            writer.WriteNullable(path);
            writer.WriteNullable7BitEncodedInt32(timeoutSeconds);
            writer.WriteNullable(command);
            writer.WriteNullable(transactionId);
            var arguments = argumentListGetter?.Invoke()?.ToArray();
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

    public void OnContextIoCommandEnd(IProcess process, long uid, IoCommandKind kind, long? affectedDataCount, Exception ex)
    {
        SendDiagnostics(DiagnosticsEventKind.IoCommandEnd, writer =>
        {
            writer.Write7BitEncodedInt64(uid);
            writer.WriteNullable7BitEncodedInt64(affectedDataCount);
            writer.WriteNullable(ex?.FormatExceptionWithDetails());
        });
    }

    public void OnContextClosed()
    {
        SendDiagnostics(DiagnosticsEventKind.ContextEnded, null);

        Flush();
    }
}