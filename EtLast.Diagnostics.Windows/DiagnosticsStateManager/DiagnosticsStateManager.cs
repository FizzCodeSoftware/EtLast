namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Text.Json;
    using System.Web;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal delegate void OnSessionCreatedDelegate(Session session);
    internal delegate void OnExecutionContextCreatedDelegate(ExecutionContext executionContext);

    internal class DiagnosticsStateManager : IDisposable
    {
        public OnSessionCreatedDelegate OnSessionCreated { get; set; }
        public OnExecutionContextCreatedDelegate OnExecutionContextCreated { get; set; }

        private readonly HttpListener _listener;
        private readonly Dictionary<string, Session> _sessionList = new Dictionary<string, Session>();
        public IEnumerable<Session> Session => _sessionList.Values;

        private readonly List<Session> _newSessions = new List<Session>();
        private readonly List<ExecutionContext> _newExecutionContexts = new List<ExecutionContext>();

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

            var context = GetExecutionContext(sessionId, contextName);

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
                    "process-invocation-start" => ProcessProcessInvocationStartEvent(payload),
                    "process-invocation-end" => ProcessProcessInvocationEndEvent(payload),
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
            List<ExecutionContext> allContextList;
            List<ExecutionContext> newContexts;
            List<Session> newSessions;
            lock (_sessionList)
            {
                newSessions = new List<Session>(_newSessions);
                _newSessions.Clear();

                newContexts = new List<ExecutionContext>(_newExecutionContexts);
                _newExecutionContexts.Clear();

                allContextList = _sessionList.Values.SelectMany(x => x.ContextList).ToList();
            }

            foreach (var session in newSessions)
                OnSessionCreated?.Invoke(session);

            foreach (var executionContext in newContexts)
                OnExecutionContextCreated?.Invoke(executionContext);

            foreach (var executionContext in allContextList)
            {
                executionContext.ProcessEvents();
            }
        }

        public ExecutionContext GetExecutionContext(string sessionId, string name)
        {
            if (!_sessionList.TryGetValue(sessionId, out var session))
            {
                session = new Session(sessionId);

                lock (_sessionList)
                {
                    _sessionList.Add(sessionId, session);

                    _newSessions.Add(session);
                }
            }

            if (name == null)
                name = "/";

            if (!session.ExecutionContextListByName.TryGetValue(name, out var context))
            {
                context = new ExecutionContext(session, name);

                lock (_sessionList)
                {
                    session.ContextList.Add(context);
                    session.ExecutionContextListByName.Add(name, context);

                    _newExecutionContexts.Add(context);
                }
            }

            return context;
        }

        private static AbstractEvent ProcessProcessInvocationStartEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<ProcessInvocationStartEvent>(payload);
            return evt;
        }

        private static AbstractEvent ProcessProcessInvocationEndEvent(string payload)
        {
            var evt = JsonSerializer.Deserialize<ProcessInvocationEndEvent>(payload);
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
            if (evt.Values != null)
            {
                foreach (var value in evt.Values)
                {
                    value.CalculateValue();
                }
            }

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