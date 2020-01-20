namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Text.Json;
    using System.Web;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal delegate void OnNewEventArrivedDelegate(SessionContext context, AbstractEvent evt);
    internal delegate void OnSessionCreatedDelegate(Session session);

    internal class DiagnosticsStateManager : IDisposable
    {
        public OnSessionCreatedDelegate OnSessionCreated { get; set; }
        public OnNewEventArrivedDelegate OnNewEventArrived { get; set; }

        private readonly HttpListener _listener;
        private readonly Dictionary<string, Session> _sessions = new Dictionary<string, Session>();
        public IEnumerable<Session> Session => _sessions.Values;

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

                    HandleRequest(request.HttpMethod, context.Request.Url.AbsolutePath, body, query);
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

        private void HandleRequest(string httpMethod, string absoluteUrl, string body, NameValueCollection query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var sessionId = query["sid"];
            if (string.IsNullOrEmpty(sessionId))
                return;

            var session = GetSession(sessionId);

            switch (absoluteUrl, httpMethod)
            {
                case ("/log", "POST"):
                    {
                        HandleLogEvent(session, body);
                        return;
                    }
                case ("/row-created", "POST"):
                    {
                        HandleRowCreatedEvent(session, body);
                        return;
                    }
                case ("/row-owner-changed", "POST"):
                    {
                        HandleRowOwnerChangedEvent(session, body);
                        return;
                    }
                case ("/row-value-changed", "POST"):
                    {
                        HandleRowValueChangedEvent(session, body);
                        return;
                    }
                case ("/row-stored", "POST"):
                    {
                        HandleRowStoredEvent(session, body);
                        return;
                    }
            }
        }

        private Session GetSession(string sessionId)
        {
            if (!_sessions.TryGetValue(sessionId, out var session))
            {
                session = new Session(sessionId);
                _sessions.Add(sessionId, session);

                OnSessionCreated?.Invoke(session);
            }

            return session;
        }

        private void HandleRowCreatedEvent(Session session, string body)
        {
            var evt = JsonSerializer.Deserialize<RowCreatedEvent>(body);

            if (evt.Values != null)
            {
                foreach (var arg in evt.Values)
                {
                    arg.CalculateValue();
                }
            }

            var context = session.AddEvent(evt);
            OnNewEventArrived?.Invoke(context, evt);
        }

        private void HandleRowOwnerChangedEvent(Session session, string body)
        {
            var evt = JsonSerializer.Deserialize<RowOwnerChangedEvent>(body);
            var context = session.AddEvent(evt);
            OnNewEventArrived?.Invoke(context, evt);
        }

        private void HandleRowValueChangedEvent(Session session, string body)
        {
            var evt = JsonSerializer.Deserialize<RowValueChangedEvent>(body);
            evt.CurrentValue.CalculateValue();

            var context = session.AddEvent(evt);
            OnNewEventArrived?.Invoke(context, evt);
        }

        private void HandleRowStoredEvent(Session session, string body)
        {
            var evt = JsonSerializer.Deserialize<RowStoredEvent>(body);
            var context = session.AddEvent(evt);
            OnNewEventArrived?.Invoke(context, evt);
        }

        private void HandleLogEvent(Session session, string body)
        {
            var evt = JsonSerializer.Deserialize<LogEvent>(body);
            if (evt.Arguments != null)
            {
                foreach (var arg in evt.Arguments)
                {
                    arg.CalculateValue();
                }
            }

            var context = session.AddEvent(evt);
            OnNewEventArrived?.Invoke(context, evt);
        }

        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing && _listener != null)
                {
                    _listener.Stop();
                    _listener.Close();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}