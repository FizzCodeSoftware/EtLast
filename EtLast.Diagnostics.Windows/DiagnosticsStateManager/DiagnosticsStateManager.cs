namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
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

                if (context.Request.Url.AbsolutePath == "/diag" && request.HttpMethod == "POST")
                {
                    using (var ms = new MemoryStream())
                    {
                        context.Request.InputStream.CopyTo(ms);
                        ms.Position = 0;

                        using (var bodyReader = new BinaryReader(ms, Encoding.UTF8))
                        {
                            var query = HttpUtility.ParseQueryString(request.Url.Query);
                            HandleRequest(bodyReader, query);
                        }
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

        private void HandleRequest(BinaryReader bodyReader, NameValueCollection query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var sessionId = query["sid"];
            if (string.IsNullOrEmpty(sessionId))
                return;

            var contextName = query["ctx"];
            var context = GetExecutionContext(sessionId, contextName);

            var events = new List<AbstractEvent>();

            while (bodyReader.BaseStream.Position < bodyReader.BaseStream.Length)
            {
                var eventKind = (DiagnosticsEventKind)bodyReader.ReadByte();
                var timestamp = bodyReader.ReadInt64();

                var abstractEvent = eventKind switch
                {
                    DiagnosticsEventKind.Log => ProcessLogEvent(bodyReader),
                    DiagnosticsEventKind.RowCreated => ProcessRowCreatedEvent(bodyReader),
                    DiagnosticsEventKind.RowOwnerChanged => ProcessRowOwnerChangedEvent(bodyReader),
                    DiagnosticsEventKind.RowValueChanged => ProcessRowValueChangedEvent(bodyReader),
                    DiagnosticsEventKind.RowStored => ProcessRowStoredEvent(bodyReader),
                    DiagnosticsEventKind.ContextCountersUpdated => ProcessContextCountersUpdatedEvent(bodyReader),
                    DiagnosticsEventKind.ProcessInvocationStart => ProcessProcessInvocationStartEvent(bodyReader),
                    DiagnosticsEventKind.ProcessInvocationEnd => ProcessProcessInvocationEndEvent(bodyReader),
                    DiagnosticsEventKind.DataStoreCommand => ProcessDataStoreCommandEvent(bodyReader),
                    _ => null,
                };

                if (abstractEvent != null)
                {
                    abstractEvent.Timestamp = timestamp;
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

        private static AbstractEvent ProcessProcessInvocationStartEvent(BinaryReader reader)
        {
            return new ProcessInvocationStartEvent
            {
                InvocationUID = reader.ReadInt32(),
                InstanceUID = reader.ReadInt32(),
                InvocationCounter = reader.ReadInt32(),
                Type = reader.ReadString(),
                Kind = (ProcessKind)reader.ReadByte(),
                Name = reader.ReadString(),
                Topic = reader.ReadNullableString(),
                CallerInvocationUID = reader.ReadNullableInt32()
            };
        }

        private static AbstractEvent ProcessProcessInvocationEndEvent(BinaryReader reader)
        {
            return new ProcessInvocationEndEvent
            {
                InvocationUID = reader.ReadInt32(),
                ElapsedMilliseconds = reader.ReadInt64()
            };
        }

        private static AbstractEvent ProcessDataStoreCommandEvent(BinaryReader reader)
        {
            var evt = new DataStoreCommandEvent
            {
                ProcessInvocationUID = reader.ReadInt32(),
                Kind = (DataStoreCommandKind)reader.ReadByte(),
                Location = reader.ReadNullableString(),
                Command = reader.ReadString(),
                TransactionId = reader.ReadNullableString()
            };

            var argCount = reader.ReadUInt16();
            if (argCount > 0)
            {
                evt.Arguments = new KeyValuePair<string, object>[argCount];
                for (var i = 0; i < argCount; i++)
                {
                    var name = string.Intern(reader.ReadString());
                    var value = reader.ReadObject();
                    evt.Arguments[i] = new KeyValuePair<string, object>(name, value);
                }
            }

            return evt;
        }

        private static AbstractEvent ProcessRowCreatedEvent(BinaryReader reader)
        {
            var evt = new RowCreatedEvent
            {
                ProcessInvocationUID = reader.ReadInt32(),
                RowUid = reader.ReadInt32()
            };

            var valueCount = reader.ReadUInt16();
            if (valueCount > 0)
            {
                evt.Values = new KeyValuePair<string, object>[valueCount];
                for (var i = 0; i < valueCount; i++)
                {
                    var column = string.Intern(reader.ReadString());
                    var value = reader.ReadObject();
                    evt.Values[i] = new KeyValuePair<string, object>(column, value);
                }
            }

            return evt;
        }

        private static AbstractEvent ProcessRowOwnerChangedEvent(BinaryReader reader)
        {
            return new RowOwnerChangedEvent
            {
                RowUid = reader.ReadInt32(),
                PreviousProcessInvocationUID = reader.ReadInt32(),
                NewProcessInvocationUID = reader.ReadNullableInt32()
            };
        }

        private static AbstractEvent ProcessRowValueChangedEvent(BinaryReader reader)
        {
            var evt = new RowValueChangedEvent
            {
                RowUid = reader.ReadInt32(),
                ProcessInvocationUID = reader.ReadNullableInt32()
            };

            var valueCount = reader.ReadUInt16();
            if (valueCount > 0)
            {
                evt.Values = new KeyValuePair<string, object>[valueCount];
                for (var i = 0; i < valueCount; i++)
                {
                    var column = string.Intern(reader.ReadString());
                    var value = reader.ReadObject();
                    evt.Values[i] = new KeyValuePair<string, object>(column, value);
                }
            }

            return evt;
        }

        private static AbstractEvent ProcessRowStoredEvent(BinaryReader reader)
        {
            var evt = new RowStoredEvent
            {
                RowUid = reader.ReadInt32(),
                ProcessInvocationUID = reader.ReadInt32()
            };

            var locationCount = reader.ReadUInt16();
            if (locationCount > 0)
            {
                evt.Locations = new KeyValuePair<string, string>[locationCount];
                for (var i = 0; i < locationCount; i++)
                {
                    var key = string.Intern(reader.ReadString());
                    var value = string.Intern(reader.ReadString());
                    evt.Locations[i] = new KeyValuePair<string, string>(key, value);
                }
            }

            return evt;
        }

        private static AbstractEvent ProcessContextCountersUpdatedEvent(BinaryReader reader)
        {
            var evt = new ContextCountersUpdatedEvent();
            int counterCount = reader.ReadUInt16();
            evt.Counters = new Counter[counterCount];
            for (var i = 0; i < counterCount; i++)
            {
                evt.Counters[i] = new Counter()
                {
                    Name = string.Intern(reader.ReadString()),
                    Value = reader.ReadInt64(),
                    ValueType = (StatCounterValueType)reader.ReadByte(),
                };
            }

            return evt;
        }

        private static AbstractEvent ProcessLogEvent(BinaryReader reader)
        {
            var evt = new LogEvent
            {
                Text = reader.ReadString(),
                Severity = (LogSeverity)reader.ReadByte(),
                ProcessInvocationUID = reader.ReadNullableInt32()
            };

            var argCount = reader.ReadUInt16();
            if (argCount > 0)
            {
                evt.Arguments = new KeyValuePair<string, object>[argCount];
                for (var i = 0; i < argCount; i++)
                {
                    var key = string.Intern(reader.ReadString());
                    var value = reader.ReadObject();
                    evt.Arguments[i] = new KeyValuePair<string, object>(key, value);
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