#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Specialized;
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Text;
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

                    HandleRequest(context.Request.Url.AbsolutePath, body, query);
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

        private void HandleRequest(string absoluteUrl, string body, NameValueCollection query)
        {
            switch (absoluteUrl)
            {
                case "/favicon.ico":
                    return;
                case "/log":
                    NewLog(query);
                    return;
            }

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText(absoluteUrl);
                _rb.AppendText(Environment.NewLine);

                if (!string.IsNullOrEmpty(body))
                {
                    _rb.AppendText(body);
                    _rb.AppendText(Environment.NewLine);
                }

                foreach (var queryKey in query.AllKeys)
                {
                    _rb.AppendText(queryKey + " = " + query.Get(queryKey));
                    _rb.AppendText(Environment.NewLine);
                }
            });
        }

        private void NewLog(NameValueCollection query)
        {
            var entry = new LogEntry
            {
                ContextUid = query["contextUid"],
                ContextName = query["contextName"].Split(',', StringSplitOptions.RemoveEmptyEntries),
                CallerUid = query["callerUid"],
                CallerName = query["callerName"],
                OperationType = query["opType"],
                OperationNumber = query["opNum"],
                OperationName = query["opName"],
                Text = query["text"],
                ForOps = query["forOps"] == "1",
            };

            // http://localhost:8642/log?contextName=LoadData,PreLoaderPlugin&moduleName=hello&arguments=int|1123,bool|1,datetime|700000000000000000

            if (Enum.TryParse<LogSeverity>(query["severity"], out var severity))
                entry.Severity = severity;

            var args = (query["arguments"] ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries);

            foreach (var arg in args)
            {
                var argParts = arg.Split('|', StringSplitOptions.RemoveEmptyEntries);
                var argType = argParts[0];
                var strValue = argParts[1];

                object v = null;

                switch (argType)
                {
                    case "string":
                        v = strValue;
                        break;
                    case "bool":
                        v = strValue == "1";
                        break;
                    case "int":
                        v = int.Parse(strValue, CultureInfo.InvariantCulture);
                        break;
                    case "long":
                        v = long.Parse(strValue, CultureInfo.InvariantCulture);
                        break;
                    case "float":
                        v = float.Parse(strValue, CultureInfo.InvariantCulture);
                        break;
                    case "double":
                        v = double.Parse(strValue, CultureInfo.InvariantCulture);
                        break;
                    case "decimal":
                        v = decimal.Parse(strValue, CultureInfo.InvariantCulture);
                        break;
                    case "datetime":
                        v = new DateTime(long.Parse(strValue, CultureInfo.InvariantCulture));
                        break;
                    case "timespan":
                        v = TimeSpan.FromMilliseconds(long.Parse(strValue, CultureInfo.InvariantCulture));
                        break;
                }

                entry.Arguments.Add(v);
            }

            lock (_rootCollection)
            {
                var collection = GetCollection(entry.ContextName);
                collection.LogEntries.Add(entry);
            }

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText("LOG");
                _rb.AppendText(Environment.NewLine);
            });
        }

        private Collection GetCollection(string[] names)
        {
            var collection = _rootCollection;

            for (var i = 0; i < names.Length; i++)
            {
                collection = collection.GetChildCollection(names[i]);
            }

            return collection;
        }
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed