#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Specialized;
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
        private readonly Collection _rootCollection = new Collection() { Name = "root" };

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
                    NewLog(body);
                    return;
            }
        }

        private void NewLog(string body)
        {
            var logEvent = JsonSerializer.Deserialize<Diagnostics.Interface.LogEvent>(body);

            if (logEvent.Arguments != null)
            {
                foreach (var arg in logEvent.Arguments)
                {
                    arg.CalculateValue();
                }
            }

            lock (_rootCollection)
            {
                var collection = GetCollection(logEvent.ContextName);
                collection.LogEntries.Add(logEvent);
            }

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText("[LOG] [" + logEvent.Severity + "] [" + string.Join("/", logEvent.ContextName) + "] ");

                if (logEvent.CallerUid != null)
                {
                    _rb.AppendText("<" + logEvent.CallerName + "> ");
                }

                if (logEvent.OperationType != null)
                {
                    _rb.AppendText("(" + logEvent.OperationName + "/#" + logEvent.OperationNumber + ") ");
                }

                _rb.AppendText(logEvent.Text);

                if (logEvent.Arguments != null)
                {
                    _rb.AppendText(" ::: " + string.Join(", ", logEvent.Arguments.Select(x => x.Value == null ? "NULL" : (x.Value.ToString() + " (" + x.Value.GetType().Name + ")"))));
                }

                _rb.AppendText(Environment.NewLine);
            });
        }

        private Collection GetCollection(string[] names)
        {
            var collection = _rootCollection;

            for (var i = 0; i < names.Length; i++)
            {
                if (!collection.ChildCollectionsByName.TryGetValue(names[i], out var child))
                {
                    child = new Collection()
                    {
                        Name = names[i],
                        ParentCollection = collection,
                    };

                    collection.ChildCollections.Add(child);
                    collection.ChildCollectionsByName.Add(names[i], child);
                }

                collection = child;
            }

            return collection;
        }
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed