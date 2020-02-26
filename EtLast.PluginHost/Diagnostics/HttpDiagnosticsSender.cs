namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using FizzCode.EtLast.Diagnostics.Interface;

    public class HttpDiagnosticsSender : IDiagnosticsSender
    {
        public int MaxCommunicationErrorCount { get; } = 2;
        private readonly Uri _uri;
        private HttpClient _client;
        private readonly Thread _workerThread;
        private readonly string _sessionId;
        private readonly string _contextName;
        private ExtendedBinaryWriter _currentWriter;
        private ExtendedBinaryWriter _currentDictionaryWriter;
        private readonly object _currentWriterLock = new object();
        private readonly Dictionary<string, int> _textDictionary = new Dictionary<string, int>();
        private bool _finished;
        private int _communicationErrorCount;

        public HttpDiagnosticsSender(string sessionId, string contextName, Uri diagnosticsUri)
        {
            _sessionId = sessionId;
            _contextName = contextName;
            _uri = diagnosticsUri;
            _client = new HttpClient
            {
                Timeout = TimeSpan.FromMilliseconds(5000),
            };

            SendWriter(null);

            _workerThread = new Thread(WorkerMethod);
            _workerThread.Start();
        }

        private void WorkerMethod()
        {
            var swLastSent = Stopwatch.StartNew();

            while (!_finished && (_communicationErrorCount <= MaxCommunicationErrorCount))
            {
                ExtendedBinaryWriter dictionaryWriterToSend = null;
                ExtendedBinaryWriter writerToSend = null;
                lock (_currentWriterLock)
                {
                    if (_currentWriter != null
                        && (_currentWriter.BaseStream.Length >= 1024 * 1024
                            || (_currentWriter.BaseStream.Length > 0 && swLastSent.ElapsedMilliseconds > 500)))
                    {
                        dictionaryWriterToSend = _currentDictionaryWriter;
                        writerToSend = _currentWriter;
                        _currentDictionaryWriter = null;
                        _currentWriter = null;
                    }
                }

                if (dictionaryWriterToSend != null)
                    SendWriter(dictionaryWriterToSend);

                if (writerToSend != null)
                    SendWriter(writerToSend);

                Thread.Sleep(10);
            }

            if (_currentDictionaryWriter != null)
            {
                if (_currentDictionaryWriter.BaseStream.Length > 0 && _communicationErrorCount <= MaxCommunicationErrorCount)
                {
                    SendWriter(_currentDictionaryWriter);
                }

                _currentDictionaryWriter = null;
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

            var fullUri = new Uri(_uri, "diag?sid=" + _sessionId + (_contextName != null ? "&ctx=" + _contextName : ""));

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
                        throw new Exception("SHT");
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

        public int GetTextDictionaryKey(string text)
        {
            if (text == null)
                return 0;

            lock (_textDictionary)
            {
                if (!_textDictionary.TryGetValue(text, out var key))
                {
                    key = _textDictionary.Count + 1;
                    _textDictionary.Add(text, key);

#pragma warning disable RCS1180 // Inline lazy initialization.
                    if (_currentDictionaryWriter == null)
                        _currentDictionaryWriter = new ExtendedBinaryWriter(new MemoryStream(), Encoding.UTF8);
#pragma warning restore RCS1180 // Inline lazy initialization.

                    _currentDictionaryWriter.Write((byte)DiagnosticsEventKind.TextDictionaryKeyAdded);
                    _currentDictionaryWriter.Write7BitEncodedInt(key);
                    _currentDictionaryWriter.WriteNullable(text);
                }

                return key;
            }
        }

        public void SendDiagnostics(DiagnosticsEventKind kind, Action<ExtendedBinaryWriter> writerDelegate)
        {
            if (_communicationErrorCount > MaxCommunicationErrorCount)
                return;

            lock (_currentWriterLock)
            {
                if (_finished)
                    throw new Exception("unexpected call of " + nameof(SendDiagnostics));

#pragma warning disable RCS1180 // Inline lazy initialization.
                if (_currentWriter == null)
                    _currentWriter = new ExtendedBinaryWriter(new MemoryStream(), Encoding.UTF8);
#pragma warning restore RCS1180 // Inline lazy initialization.

                _currentWriter.Write((byte)kind);
                var eventDataLengthPos = (int)_currentWriter.BaseStream.Position;
                _currentWriter.Write(0);

                var startPos = (int)_currentWriter.BaseStream.Position;
                _currentWriter.Write(DateTime.Now.Ticks);

                writerDelegate?.Invoke(_currentWriter);
                var endPos = (int)_currentWriter.BaseStream.Position;
                _currentWriter.Seek(eventDataLengthPos, SeekOrigin.Begin);
                _currentWriter.Write(endPos - startPos);
                _currentWriter.Seek(endPos, SeekOrigin.Begin);
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

                    _currentDictionaryWriter?.Dispose();
                    _currentDictionaryWriter = null;
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