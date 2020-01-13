namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Concurrent;
    using System.Net.Http;
    using System.Text;
    using System.Text.Json;
    using System.Threading;

    public class HttpDiagnosticsSender : IDiagnosticsSender
    {
        private readonly Uri _uri;
        private HttpClient _client;
        private BlockingCollection<Tuple<string, object>> _queue = new BlockingCollection<Tuple<string, object>>();
        private int _id;
        private readonly Thread _workerThread;

        public HttpDiagnosticsSender(Uri diagnosticsUri)
        {
            _uri = diagnosticsUri;
            _client = new HttpClient
            {
                Timeout = TimeSpan.FromMilliseconds(1000),
            };

            _workerThread = new Thread(WorkerMethod);
            _workerThread.Start();
        }

        private void WorkerMethod()
        {
            var consumer = _queue.GetConsumingEnumerable();

            foreach (var element in consumer)
            {
                try
                {
                    var id = Interlocked.Increment(ref _id);

                    var fullUri = new Uri(_uri, element.Item1 + "?num=" + id);
                    var jsonContent = JsonSerializer.Serialize(element.Item2);

                    using (var textContent = new StringContent(jsonContent, Encoding.UTF8, "application/json"))
                    {
                        var response = _client.PostAsync(fullUri, textContent).Result;
                        var responseBody = response.Content.ReadAsStringAsync().Result;
                        if (responseBody != "ACK")
                        {
                            throw new Exception("ohh");
                        }
                    }
                }
                catch (Exception)
                {
                }
            }
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