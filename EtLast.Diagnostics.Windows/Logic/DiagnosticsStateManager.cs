namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Text.Json;
    using System.Web;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal delegate void OnSessionCreatedDelegate(Session session);

    internal class DiagnosticsStateManager : IDisposable
    {
        public OnSessionCreatedDelegate OnSessionCreated { get; set; }

        private readonly HttpListener _listener;
        private readonly Dictionary<string, Session> _sessionList = new Dictionary<string, Session>();
        public IEnumerable<Session> Session => _sessionList.Values;

        public DiagnosticsStateManager(string uriPrefix)
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add(uriPrefix);
        }

        public void Start()
        {
            _listener.Start();
            _listener.BeginGetContext(ListenerCallBack, null);
        }

        private void ListenerCallBack(IAsyncResult result)
        {
            if (_listener == null)
                return;

            try
            {
                var context = _listener.EndGetContext(result);

                var request = context.Request;
                var response = context.Response;

                using (var bodyReader = new StreamReader(context.Request.InputStream))
                {
                    var body = bodyReader.ReadToEnd();
                    var query = HttpUtility.ParseQueryString(request.Url.Query);

                    if (context.Request.Url.AbsolutePath == "/diag" && request.HttpMethod == "POST")
                    {
                        HandleRequest(body, query);
                    }
                }

                var responseBytes = Encoding.UTF8.GetBytes("ACK");
                context.Response.StatusCode = 200;
                context.Response.KeepAlive = false;
                context.Response.ContentLength64 = responseBytes.Length;

                context.Response.OutputStream.Write(responseBytes, 0, responseBytes.Length);
                context.Response.Close();
            }
            catch (Exception)
            {
            }
            finally
            {
                _listener.BeginGetContext(ListenerCallBack, null);
            }
        }

        private void HandleRequest(string body, NameValueCollection query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var sessionId = query["sid"];
            if (string.IsNullOrEmpty(sessionId))
                return;

            var contextName = query["ctx"];
            var session = GetSession(sessionId);
            var context = session.GetExecutionContext(contextName);

            using var contentReader = new StringReader(body);
            var eventCount = int.Parse(contentReader.ReadLine(), CultureInfo.InvariantCulture);

            var events = new List<AbstractEvent>(eventCount);
            for (var i = 0; i < eventCount; i++)
            {
                var eventType = contentReader.ReadLine();
                var payloadLength = int.Parse(contentReader.ReadLine(), CultureInfo.InvariantCulture);
                var payloadChars = new char[payloadLength];
                contentReader.ReadBlock(payloadChars, 0, payloadLength);
                contentReader.ReadLine();
                var payload = new string(payloadChars);

                var abstractEvent = eventType switch
                {
                    "log" => ProcessLogEvent(payload),
                    "row-created" => ProcessRowCreatedEvent(payload),
                    "row-owner-changed" => ProcessRowOwnerChangedEvent(payload),
                    "row-value-changed" => ProcessRowValueChangedEvent(payload),
                    "row-stored" => ProcessRowStoredEvent(payload),
                    "context-counters-updated" => ProcessContextCountersUpdatedEvent(payload),
                    "process-created" => ProcessProcessCreatedEvent(payload),
                    "operation-created" => ProcessOperationCreatedEvent(payload),
                    "data-store-command" => ProcessDataStoreCommandEvent(payload),
                    _ => null,
                };

                if (abstractEvent != null)
                {
                    if (context.StartedOn == null)
                    {
                        context.SetStartedOn(new DateTime(abstractEvent.Timestamp));
                    }

                    events.Add(abstractEvent);
                }
            }

            context.AddUnprocessedEvents(events);
        }

        public void ProcessEvents()
        {
            foreach (var session in _sessionList.Values)
            {
                foreach (var context in session.ContextList)
                {
                    context.ProcessEvents();
                }
            }
        }

        private Session GetSession(string sessionId)
        {
            if (!_sessionList.TryGetValue(sessionId, out var session))
            {
                session = new Session(sessionId);
                _sessionList.Add(sessionId, session);

                OnSessionCreated?.Invoke(session);
            }

            return session;
        }

        private static AbstractEvent ProcessProcessCreatedEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<ProcessCreatedEvent>(payload);
            return evt;
        }

        private static AbstractEvent ProcessOperationCreatedEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<OperationCreatedEvent>(payload);
            return evt;
        }

        private static AbstractEvent ProcessDataStoreCommandEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<DataStoreCommandEvent>(payload);
            if (evt.Arguments != null)
            {
                foreach (var arg in evt.Arguments)
                {
                    arg.CalculateValue();
                }
            }

            return evt;
        }

        private static AbstractEvent ProcessRowCreatedEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<RowCreatedEvent>(payload);

            if (evt.Values != null)
            {
                foreach (var value in evt.Values)
                {
                    value.CalculateValue();
                }
            }

            return evt;
        }

        private static AbstractEvent ProcessRowOwnerChangedEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<RowOwnerChangedEvent>(payload);
            return evt;
        }

        private static AbstractEvent ProcessRowValueChangedEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<RowValueChangedEvent>(payload);
            evt.CurrentValue.CalculateValue();
            return evt;
        }

        private static AbstractEvent ProcessRowStoredEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<RowStoredEvent>(payload);
            return evt;
        }

        private static AbstractEvent ProcessContextCountersUpdatedEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<ContextCountersUpdatedEvent>(payload);
            return evt;
        }

        private static AbstractEvent ProcessLogEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<LogEvent>(payload);
            if (evt.Arguments != null)
            {
                foreach (var arg in evt.Arguments)
                {
                    arg.CalculateValue();
                }
            }

            return evt;
        }

        private bool _disposed;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing && _listener != null)
                {
                    _listener.Stop();
                    _listener.Close();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}