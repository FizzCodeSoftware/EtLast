namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net.Http;
    using System.Text;
    using System.Text.Json;
    using System.Threading;

    public class HttpDiagnosticsSender : IDiagnosticsSender
    {
        private readonly Uri _uri;
        private HttpClient _client;
        private BlockingCollection<Tuple<string, object>> _queue = new BlockingCollection<Tuple<string, object>>();
        private readonly Thread _workerThread;
        private readonly string _sessionId;
        private readonly string _contextName;

        public HttpDiagnosticsSender(string sessionId, string contextName, Uri diagnosticsUri)
        {
            _sessionId = sessionId;
            _contextName = contextName;
            _uri = diagnosticsUri;
            _client = new HttpClient
            {
                Timeout = TimeSpan.FromMilliseconds(1500),
            };

            _workerThread = new Thread(WorkerMethod);
            _workerThread.Start();
        }

        private void WorkerMethod()
        {
            var consumer = _queue.GetConsumingEnumerable();
            var swLastSent = Stopwatch.StartNew();

            var buffer = new List<Tuple<string, object>>();
            foreach (var element in consumer)
            {
                try
                {
                    buffer.Add(element);
                    if (buffer.Count >= 1000 || swLastSent.ElapsedMilliseconds > 500)
                    {
                        SendBuffer(buffer);
                        swLastSent.Restart();
                    }
                }
                catch (Exception)
                {
                }
            }

            SendBuffer(buffer);
        }

        private void SendBuffer(List<Tuple<string, object>> buffer)
        {
            var fullUri = new Uri(_uri, "diag?sid=" + _sessionId + (_contextName != null ? "&ctx=" + _contextName : ""));
            var builder = new StringBuilder();
            builder.AppendLine(buffer.Count.ToString("D", CultureInfo.InvariantCulture));
            foreach (var element in buffer)
            {
                var type = element.Item1;
                var jsonContent = JsonSerializer.Serialize(element.Item2);
                builder.AppendLine(type);
                builder.AppendLine(jsonContent.Length.ToString("D", CultureInfo.InvariantCulture));
                builder.AppendLine(jsonContent);
            }

            var content = builder.ToString();
            Console.WriteLine("send diagnostics context, payload size = " + content.Length);

            using (var textContent = new StringContent(content, Encoding.UTF8, "application/json"))
            {
                try
                {
                    var response = _client.PostAsync(fullUri, textContent).Result;
                    var responseBody = response.Content.ReadAsStringAsync().Result;
                }
                catch (Exception)
                {
                }
            }

            buffer.Clear();
        }

        public void SendDiagnostics(string category, object content)
        {
            if (_queue.IsAddingCompleted)
                throw new Exception("unexpected call of " + nameof(SendDiagnostics));

            _queue.Add(new Tuple<string, object>(category, content));
        }

        private bool _isDisposed;

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _client?.Dispose();
                    _queue?.Dispose();

                    _client = null;
                    _queue = null;
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
            _queue.CompleteAdding();
            _workerThread.Join();
        }
    }
}