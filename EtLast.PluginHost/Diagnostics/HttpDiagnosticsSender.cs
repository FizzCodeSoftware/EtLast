namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using FizzCode.EtLast.Diagnostics.Interface;

    public class HttpDiagnosticsSender : IDiagnosticsSender
    {
        private readonly Uri _uri;
        private HttpClient _client;
        private readonly Thread _workerThread;
        private readonly string _sessionId;
        private readonly string _contextName;
        private BinaryWriter _currentWriter;
        private readonly object _currentWriterLock = new object();
        private bool _finished;

        public HttpDiagnosticsSender(string sessionId, string contextName, Uri diagnosticsUri)
        {
            _sessionId = sessionId;
            _contextName = contextName;
            _uri = diagnosticsUri;
            _client = new HttpClient
            {
                Timeout = TimeSpan.FromMilliseconds(1500),
            };

            SendWriter(null);

            _workerThread = new Thread(WorkerMethod);
            _workerThread.Start();
        }

        private void WorkerMethod()
        {
            var swLastSent = Stopwatch.StartNew();

            while (!_finished)
            {
                BinaryWriter writerToSend = null;
                lock (_currentWriterLock)
                {
                    if (_currentWriter != null &&
                        (_currentWriter.BaseStream.Length >= 1024 * 1024
                        || (_currentWriter.BaseStream.Length > 0 && swLastSent.ElapsedMilliseconds > 500)))
                    {
                        writerToSend = _currentWriter;
                        _currentWriter = null;
                    }
                }

                if (writerToSend != null)
                {
                    writerToSend.Flush();
                    SendWriter(writerToSend);
                }

                Thread.Sleep(10);
            }

            if (_currentWriter != null)
            {
                _currentWriter.Flush();
                if (_currentWriter.BaseStream.Length > 0)
                {
                    SendWriter(_currentWriter);
                }

                _currentWriter = null;
            }
        }

        private void SendWriter(BinaryWriter writer)
        {
            var fullUri = new Uri(_uri, "diag?sid=" + _sessionId + (_contextName != null ? "&ctx=" + _contextName : ""));

            var binaryContent = writer != null
                ? (writer.BaseStream as MemoryStream).ToArray()
                : Array.Empty<byte>();

            using (var content = new ByteArrayContent(binaryContent))
            {
                try
                {
                    var task = Task.Run(() => _client.PostAsync(fullUri, content));
                    task.Wait();
                    var response = task.Result;
                    var responseBody = response.Content.ReadAsStringAsync().Result;
                    if (responseBody != "ACK")
                    {
                        throw new Exception("SHT");
                    }
                }
                catch (Exception)
                {
                }
            }

            writer?.BaseStream.Dispose();
            writer?.Dispose();
        }

        public void SendDiagnostics(DiagnosticsEventKind kind, Action<BinaryWriter> writerDelegate)
        {
            lock (_currentWriterLock)
            {
                if (_finished)
                    throw new Exception("unexpected call of " + nameof(SendDiagnostics));

#pragma warning disable RCS1180 // Inline lazy initialization.
                if (_currentWriter == null)
                    _currentWriter = new BinaryWriter(new MemoryStream(), Encoding.UTF8);
#pragma warning restore RCS1180 // Inline lazy initialization.

                _currentWriter.Write((byte)kind);
                _currentWriter.Write(DateTime.Now.Ticks);
                writerDelegate?.Invoke(_currentWriter);
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
    }
}