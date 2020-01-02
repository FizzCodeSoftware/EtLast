#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Specialized;
    using System.Globalization;
    using System.IO;
    using System.Linq;
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
            switch (absoluteUrl, httpMethod)
            {
                case ("/favicon.ico", "GET"):
                    return;
                case ("/log", "POST"):
                    NewLog(query, body);
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

        private void NewLog(NameValueCollection query, string body)
        {
            var entry = new LogEntry
            {
                ContextName = query["contextName"].Split(',', StringSplitOptions.RemoveEmptyEntries),
                CallerUid = query["callerUid"],
                CallerName = query["callerName"],
                OperationType = query["opType"],
                OperationNumber = query["opNum"],
                OperationName = query["opName"],
                ForOps = query["forOps"] == "1",
            };

            if (Enum.TryParse<LogSeverity>(query["severity"], out var severity))
                entry.Severity = severity;

            using var sr = new StringReader(body);
            var textDescriptor = sr.ReadLine().Split(',', StringSplitOptions.RemoveEmptyEntries);
            var textLength = int.Parse(textDescriptor[0], CultureInfo.InvariantCulture);
            var chars = new char[textLength];
            sr.ReadBlock(chars, 0, textLength);
            sr.ReadLine();
            entry.Text = new string(chars);

            var argumentCount = int.Parse(sr.ReadLine(), CultureInfo.InvariantCulture);
            for (var i = 0; i < argumentCount; i++)
            {
                var argDescriptor = sr.ReadLine().Split(',', StringSplitOptions.RemoveEmptyEntries);
                var isNull = argDescriptor[2];
                if (isNull == "1")
                {
                    entry.Arguments.Add(null);
                    continue;
                }

                var argType = argDescriptor[0];
                var argLength = int.Parse(argDescriptor[1], CultureInfo.InvariantCulture);

                chars = new char[argLength];
                sr.ReadBlock(chars, 0, argLength);
                sr.ReadLine();
                var strValue = new string(chars);

                var v = argType switch
                {
                    "string" => strValue,
                    "bool" => strValue == "1",
                    "int" => int.Parse(strValue, CultureInfo.InvariantCulture),
                    "long" => long.Parse(strValue, CultureInfo.InvariantCulture),
                    "float" => float.Parse(strValue, CultureInfo.InvariantCulture),
                    "double" => double.Parse(strValue, CultureInfo.InvariantCulture),
                    "decimal" => decimal.Parse(strValue, CultureInfo.InvariantCulture),
                    "datetime" => new DateTime(long.Parse(strValue, CultureInfo.InvariantCulture)),
                    "timespan" => TimeSpan.FromMilliseconds(long.Parse(strValue, CultureInfo.InvariantCulture)),
                    _ => (object)strValue,
                };

                entry.Arguments.Add(v);
            }

            lock (_rootCollection)
            {
                var collection = GetCollection(entry.ContextName);
                collection.LogEntries.Add(entry);
            }

            _rb.Invoke((MethodInvoker)delegate
            {
                _rb.AppendText("[LOG] [" + entry.Severity + "] [" + string.Join("/", entry.ContextName) + "] ");

                if (entry.CallerUid != null)
                {
                    _rb.AppendText("<" + entry.CallerName + "> ");
                }

                if (entry.OperationType != null)
                {
                    _rb.AppendText("(" + entry.OperationName + "/#" + entry.OperationNumber + ") ");
                }

                _rb.AppendText(entry.Text);

                if (entry.Arguments != null)
                {
                    _rb.AppendText(" ::: " + string.Join(", ", entry.Arguments.Select(x => x == null ? "NULL" : (x.ToString() + " (" + x.GetType().Name + ")"))));
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