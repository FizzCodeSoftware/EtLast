#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
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

    public partial class MainForm : Form
    {
        private HttpListener _listener;
        private readonly RichTextBox _rb;
        private readonly Collection _rootCollection = new Collection("root");

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

            switch (absoluteUrl, httpMethod)
            {
                case ("/favicon.ico", "GET"):
                    return;
                case ("/log", "POST"):
                    {
                        var num = int.Parse(query["num"], CultureInfo.InvariantCulture);
                        HandleLogEvent(body, num);
                        return;
                    }
                case ("/row-created", "POST"):
                    {
                        var num = int.Parse(query["num"], CultureInfo.InvariantCulture);
                        HandleRowCreatedEvent(body, num);
                        return;
                    }
                case ("/row-owner-changed", "POST"):
                    {
                        var num = int.Parse(query["num"], CultureInfo.InvariantCulture);
                        HandleRowOwnerChangedEvent(body, num);
                        return;
                    }
            }
        }

        private void HandleRowCreatedEvent(string body, int num)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.RowCreatedEvent>(body);

            if (payload.Values != null)
            {
                foreach (var arg in payload.Values)
                {
                    arg.CalculateValue();
                }
            }

            lock (_rootCollection)
            {
                var collection = _rootCollection.GetCollection(payload.ContextName);
                collection.CurrentPlayBook.AddEvent(num, payload);
            }

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText("[ROW-CREATED] [" + string.Join("/", payload.ContextName) + "] ");

                if (payload.ProcessUid != null)
                {
                    _rb.AppendText("<" + payload.ProcessName + "> ");
                }

                _rb.AppendText("UID=" + payload.RowUid.ToString("D", CultureInfo.InvariantCulture));

                if (payload.Values != null)
                {
                    _rb.AppendText(", " + string.Join(", ", payload.Values.Select(x => x.Name + "=" + (x.Value == null ? "<null>" : (x.Value.ToString() + " (" + x.Value.GetType().Name + ")")))));
                }

                _rb.AppendText(Environment.NewLine);
            });
        }

        private void HandleRowOwnerChangedEvent(string body, int num)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.RowOwnerChangedEvent>(body);

            lock (_rootCollection)
            {
                var collection = _rootCollection.GetCollection(payload.ContextName);
                collection.CurrentPlayBook.AddEvent(num, payload);
            }

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText("[ROW-OWNER-CHANGED] [" + string.Join("/", payload.ContextName) + "] ");

                if (payload.NewProcessUid != null)
                {
                    _rb.AppendText("<" + payload.NewProcessName + "> ");
                }

                _rb.AppendText("UID=" + payload.RowUid.ToString("D", CultureInfo.InvariantCulture));

                _rb.AppendText(Environment.NewLine);
            });
        }

        private void HandleLogEvent(string body, int num)
        {
            var payload = JsonSerializer.Deserialize<Diagnostics.Interface.LogEvent>(body);

            if (payload.Arguments != null)
            {
                foreach (var arg in payload.Arguments)
                {
                    arg.CalculateValue();
                }
            }

            lock (_rootCollection)
            {
                var collection = _rootCollection.GetCollection(payload.ContextName);
                collection.CurrentPlayBook.AddEvent(num, payload);
            }

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText("[LOG] [" + new DateTime(payload.Timestamp).ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture) + "] [" + payload.Severity + "] [" + string.Join("/", payload.ContextName) + "] ");

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
                        text = text.Replace(arg.Name, arg.TextValue, StringComparison.InvariantCultureIgnoreCase);
                    }
                }

                _rb.AppendText(text);

                _rb.AppendText(Environment.NewLine);
            });
        }
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed