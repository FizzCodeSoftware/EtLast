using System.Net;
using System.Reflection;
using System.Web;

namespace FizzCode.EtLast.Diagnostics.Windows;

internal delegate void OnContextCreatedDelegate(DiagContext context);

internal class DiagnosticsStateManager : IDisposable
{
    public OnContextCreatedDelegate OnDiagContextCreated { get; set; }

    private readonly HttpListener _listener;
    private readonly Dictionary<long, DiagContext> _contextList = [];
    public IEnumerable<DiagContext> Contexts => _contextList.Values;

    private readonly List<DiagContext> _newContextList = [];

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
            var listenerContext = _listener.EndGetContext(result);

            var request = listenerContext.Request;
            var response = listenerContext.Response;

            var query = HttpUtility.ParseQueryString(request.Url.Query);
            var contextIdStr = query?["sid"];
            var contextId = long.Parse(contextIdStr);
            var contextName = query?["ctx"] ?? "-";

            if (string.IsNullOrEmpty(contextIdStr)
                || string.IsNullOrEmpty(contextName)
                || listenerContext.Request.Url.AbsolutePath != "/diag"
                || request.HttpMethod != "POST")
            {
                var droppedResponse = Encoding.UTF8.GetBytes("DROPPED");
                listenerContext.Response.StatusCode = 200;
                listenerContext.Response.KeepAlive = false;
                listenerContext.Response.ContentLength64 = droppedResponse.Length;
                listenerContext.Response.OutputStream.Write(droppedResponse, 0, droppedResponse.Length);
                listenerContext.Response.Close();
                return;
            }

            var now = DateTime.Now;

            if (!_contextList.TryGetValue(contextId, out var context))
            {
                var folder = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "streams", contextIdStr);
                if (!Directory.Exists(folder))
                    Directory.CreateDirectory(folder);

                File.WriteAllLines(Path.Combine(folder, "stream-info.txt"),
[
                    "id\t" + contextIdStr,
                    "name\t" + contextName,
                    "started-on\t" + now.ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                ]);

                context = new DiagContext(contextId, contextName, now, folder);
                lock (_contextList)
                {
                    _contextList.Add(contextId, context);
                    _newContextList.Add(context);
                }
            }

            using (var ms = new MemoryStream())
            {
                listenerContext.Request.InputStream.CopyTo(ms);
                ms.Position = 0;

                context.Stage(ms);
            }

            var acceptedResponse = Encoding.UTF8.GetBytes("ACK");
            listenerContext.Response.StatusCode = 200;
            listenerContext.Response.KeepAlive = false;
            listenerContext.Response.ContentLength64 = acceptedResponse.Length;

            listenerContext.Response.OutputStream.Write(acceptedResponse, 0, acceptedResponse.Length);
            listenerContext.Response.Close();
        }
        catch (Exception)
        {
        }
        finally
        {
            _listener.BeginGetContext(ListenerCallBack, null);
        }
    }

    public void ProcessEvents()
    {
        List<DiagContext> newContexts;
        lock (_contextList)
        {
            newContexts = new List<DiagContext>(_newContextList);
            _newContextList.Clear();

            foreach (var ctx in _newContextList)
                _contextList.Add(ctx.Id, ctx);
        }

        foreach (var context in newContexts)
            OnDiagContextCreated?.Invoke(context);

        foreach (var context in _contextList.Values)
        {
            context.FlushToPlaybook();
        }
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
