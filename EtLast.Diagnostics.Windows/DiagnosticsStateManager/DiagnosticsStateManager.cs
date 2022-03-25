namespace FizzCode.EtLast.Diagnostics.Windows;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Web;
using FizzCode.EtLast.Diagnostics.Interface;

internal delegate void OnSessionCreatedDelegate(DiagSession session);
internal delegate void OnDiagContextCreatedDelegate(DiagContext diagContext);

internal class DiagnosticsStateManager : IDisposable
{
    public OnSessionCreatedDelegate OnDiagSessionCreated { get; set; }
    public OnDiagContextCreatedDelegate OnDiagContextCreated { get; set; }

    private readonly HttpListener _listener;
    private readonly Dictionary<string, DiagSession> _sessionList = new();
    public IEnumerable<DiagSession> Session => _sessionList.Values;

    private readonly List<DiagSession> _newSessions = new();
    private readonly List<DiagContext> _newDiagContexts = new();

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
            var sessionId = query?["sid"];
            var contextName = query?["ctx"] ?? "session";

            if (string.IsNullOrEmpty(sessionId)
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

            if (!_sessionList.TryGetValue(sessionId, out var session))
            {
                var folder = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "incoming-sessions", now.ToString("yyyy-MM-dd - HH-mm-ss", CultureInfo.InvariantCulture) + " - " + sessionId);
                if (!Directory.Exists(folder))
                    Directory.CreateDirectory(folder);

                File.WriteAllLines(Path.Combine(folder, "session-info.txt"), new[]
{
                    "id\t" + sessionId,
                    "started-on\t" + now.ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                });

                session = new DiagSession(sessionId, folder, now);
                lock (_sessionList)
                {
                    _sessionList.Add(sessionId, session);
                    _newSessions.Add(session);
                }
            }

            if (!session.ContextListByName.TryGetValue(contextName, out var context))
            {
                var folder = Path.Combine(session.DataFolder, contextName.Replace("/", "-", StringComparison.InvariantCultureIgnoreCase));
                if (!Directory.Exists(folder))
                    Directory.CreateDirectory(folder);

                File.WriteAllLines(Path.Combine(folder, "context-info.txt"), new[]
                {
                    "name\t" + contextName,
                    "started-on\t" + now.ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                });

                context = new DiagContext(session, contextName, now, folder);

                lock (_sessionList)
                {
                    session.ContextList.Add(context);
                    session.ContextListByName.Add(contextName, context);

                    _newDiagContexts.Add(context);
                }
            }

            using (var tempMemoryStream = new MemoryStream())
            {
                listenerContext.Request.InputStream.CopyTo(tempMemoryStream);
                tempMemoryStream.Position = 0;

                context.Stage(tempMemoryStream);
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
        List<DiagContext> allContextList;
        List<DiagContext> newContexts;
        List<DiagSession> newSessions;
        lock (_sessionList)
        {
            newSessions = new List<DiagSession>(_newSessions);
            _newSessions.Clear();

            newContexts = new List<DiagContext>(_newDiagContexts);
            _newDiagContexts.Clear();

            allContextList = _sessionList.Values.SelectMany(x => x.ContextList).ToList();
        }

        foreach (var session in newSessions)
            OnDiagSessionCreated?.Invoke(session);

        foreach (var context in newContexts)
            OnDiagContextCreated?.Invoke(context);

        foreach (var context in allContextList)
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
