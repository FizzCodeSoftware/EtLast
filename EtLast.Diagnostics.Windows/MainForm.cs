#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Drawing;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Text.Json;
    using System.Web;
    using System.Windows.Forms;
    using FizzCode.EtLast;

    public partial class MainForm : Form
    {
        private HttpListener _listener;
        private readonly RichTextBox _rb;
        private readonly Dictionary<string, Session> _sessions = new Dictionary<string, Session>();

        public MainForm()
        {
            InitializeComponent();

            CreateListener();

            WindowState = FormWindowState.Maximized;

            _rb = new RichTextBox
            {
                Parent = this,
                Dock = DockStyle.Fill,
                ReadOnly = true,
                BackColor = Color.Black,
                ForeColor = Color.LightGray,
                Font = new Font("CONSOLAS", 11.0f)
            };
        }

        private void CreateListener()
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add("http://+:8642/");
            _listener.Start();
            _listener.BeginGetContext(new AsyncCallback(ListenerCallBack), null);
        }

        private void ListenerCallBack(IAsyncResult result)
        {
            if (_listener == null)
                return;

            try
            {
                var context = _listener.EndGetContext(result);

                _listener.BeginGetContext(new AsyncCallback(ListenerCallBack), null);

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

                _rb.Invoke((MethodInvoker)delegate
                {
                    _rb.AppendText("[SESSION STARTED] [" + sessionId + "]" + Environment.NewLine);
                });
            }

            return session;
        }

        private void HandleRowCreatedEvent(Session session, string body)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.RowCreatedEvent>(body);

            if (payload.Values != null)
            {
                foreach (var arg in payload.Values)
                {
                    arg.CalculateValue();
                }
            }

            var context = session.AddEvent(payload);

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText(new DateTime(payload.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-CREATED] ");

                if (payload.ProcessUid != null)
                {
                    _rb.AppendText("<" + payload.ProcessName + "> ");
                }

                _rb.AppendText("UID=" + payload.RowUid.ToString("D", CultureInfo.InvariantCulture));

                if (payload.Values != null)
                {
                    _rb.AppendText(", " + string.Join(", ", payload.Values.Select(x => x.Name + "=" + x.ToDisplayValue() + (x.Value == null ? "" : " (" + x.Value.GetType().Name + ")"))));
                }

                _rb.AppendText(Environment.NewLine);
            });
        }

        private void HandleRowOwnerChangedEvent(Session session, string body)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.RowOwnerChangedEvent>(body);

            var context = session.AddEvent(payload);

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText(new DateTime(payload.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-OWNER-CHANGED] ");

                if (payload.NewProcessUid != null)
                {
                    _rb.AppendText("<" + payload.NewProcessName + "> ");
                }

                _rb.AppendText("UID=" + payload.RowUid.ToString("D", CultureInfo.InvariantCulture));

                _rb.AppendText(Environment.NewLine);
            });
        }

        private void HandleRowValueChangedEvent(Session session, string body)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.RowValueChangedEvent>(body);

            payload.PreviousValue.CalculateValue();
            payload.CurrentValue.CalculateValue();

            var context = session.AddEvent(payload);

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText(new DateTime(payload.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-VALUE-CHANGED] ");

                if (payload.ProcessUid != null)
                {
                    _rb.AppendText("<" + payload.ProcessName + "> ");
                }

                if (payload.OperationType != null)
                {
                    _rb.AppendText("(" + payload.OperationName + "/#" + payload.OperationNumber + ") ");
                }

                _rb.AppendText("UID=" + payload.RowUid.ToString("D", CultureInfo.InvariantCulture)
                    + ", column: " + payload.Column + ", previous value: " + payload.PreviousValue.ToDisplayValue() + ", current value: " + payload.CurrentValue.ToDisplayValue());

                _rb.AppendText(Environment.NewLine);
            });
        }

        private void HandleRowStoredEvent(Session session, string body)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.RowStoredEvent>(body);

            var context = session.AddEvent(payload);

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText(new DateTime(payload.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-STORED] UID=" + payload.RowUid.ToString("D", CultureInfo.InvariantCulture) + ", location: " + string.Join(" / ", payload.Locations.Select(x => x.Key + "=" + x.Value)));
                _rb.AppendText(Environment.NewLine);
            });
        }

        private void HandleLogEvent(Session session, string body)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.LogEvent>(body);

            if (payload.Arguments != null)
            {
                foreach (var arg in payload.Arguments)
                {
                    arg.CalculateValue();
                }
            }

            var context = session.AddEvent(payload);

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText(new DateTime(payload.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [" + SeverityToShortString(payload.Severity) + "] ");

                if (payload.ProcessUid != null)
                {
                    _rb.AppendText("<" + payload.ProcessName + "> ");
                }

                if (payload.OperationType != null)
                {
                    _rb.AppendText("(" + payload.OperationName + "/#" + payload.OperationNumber + ") ");
                }

                var text = payload.Text;
                if (payload.Arguments != null)
                {
                    foreach (var arg in payload.Arguments)
                    {
                        text = text.Replace(arg.Name, arg.ToDisplayValue(), StringComparison.InvariantCultureIgnoreCase);
                    }
                }

                _rb.AppendText(text);

                _rb.AppendText(Environment.NewLine);
            });
        }

        private static string SeverityToShortString(LogSeverity severity)
        {
            return severity switch
            {
                LogSeverity.Verbose => "VRB",
                LogSeverity.Debug => "DBG",
                LogSeverity.Information => "INF",
                LogSeverity.Warning => "WRN",
                LogSeverity.Error => "ERR",
                LogSeverity.Fatal => "FTL",
                _ => severity.ToString(),
            };
        }
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed