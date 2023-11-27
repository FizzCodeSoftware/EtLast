namespace FizzCode.EtLast;

public sealed class EtlContext : IEtlContext
{
    public Type RowType { get; private set; }

    public AdditionalData AdditionalData { get; } = new AdditionalData();
    public ArgumentCollection Arguments { get; }

    public IEtlContext Context => this;
    public FlowState GetFlowState() => new(this);

    /// <summary>
    /// Returns the identifier of the context, uniqueness is not guaranteed. Example: s220530-123059-993.
    /// </summary>
    public string Id { get; } = "s" + DateTime.Now.ToString("yyMMdd-HHmmss-fff", CultureInfo.InvariantCulture);

    /// <summary>
    /// Returns the name of the context.
    /// </summary>
    public string Name { get; init; } = Guid.NewGuid().ToString("D");

    public DateTimeOffset CreatedOnUtc { get; }
    public DateTimeOffset CreatedOnLocal { get; }

    public List<IEtlContextListener> Listeners { get; } = [];

    /// <summary>
    /// Default value: 4 hours, but .NET maximizes the timeout in 10 minutes.
    /// </summary>
    public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromHours(4);

    /// <summary>
    /// Default value: 1000
    /// </summary>
    public int ElapsedMillisecondsLimitToLog { get; set; } = 1000;

    public bool IsTerminating { get; private set; }

    private readonly CancellationTokenSource _cancellationTokenSource;
    public CancellationToken CancellationToken { get; }

    private readonly List<IEtlService> _services = [];

    private long _nextRowUid;
    private long _nextProcessInstanceUid;
    private long _nextProcessInvocationUid;
    private long _nextSinkUid;
    private long _nextIoCommandUid;
    private readonly Dictionary<string, long> _sinks = [];

    private readonly List<ScopeAction> _scopeActions = [];

    public EtlContext(ArgumentCollection arguments)
    {
        SetRowType<Row>();

        _cancellationTokenSource = new CancellationTokenSource();
        CancellationToken = _cancellationTokenSource.Token;

        Arguments = arguments;

        CreatedOnLocal = DateTimeOffset.Now;
        CreatedOnUtc = CreatedOnLocal.ToUniversalTime();
    }

    public void RegisterScopeAction(ScopeAction action)
    {
        _scopeActions.Add(action);
    }

    public ScopeAction[] GetScopeActions()
    {
        return _scopeActions.ToArray();
    }

    public T Service<T>() where T : IEtlService, new()
    {
        var service = _services.OfType<T>().FirstOrDefault();
        if (service != null)
            return service;

        service = new T();
        service.Start(this);
        _services.Add(service);

        return service;
    }

    public void Terminate()
    {
        _cancellationTokenSource.Cancel();
        IsTerminating = true;
    }

    public void SetRowType<T>() where T : IRow
    {
        RowType = typeof(T);
    }

    public void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
            listener.OnLog(severity, false, transactionId, process, text, args);
    }

    public void Log(LogSeverity severity, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
            listener.OnLog(severity, false, null, process, text, args);
    }

    public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
            listener.OnLog(severity, true, null, process, text, args);
    }

    public void LogCustom(string fileName, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
            listener.OnCustomLog(false, fileName, process, text, args);
    }

    public void LogCustomOps(string fileName, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
            listener.OnCustomLog(true, fileName, process, text, args);
    }

    public long RegisterIoCommandStart(IProcess process, IoCommandKind kind, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra)
    {
        var uid = Interlocked.Increment(ref _nextIoCommandUid);
        foreach (var listener in Listeners)
            listener.OnContextIoCommandStart(uid, kind, null, null, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageExtra);

        return uid;
    }

    public long RegisterIoCommandStartWithLocation(IProcess process, IoCommandKind kind, string location, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra)
    {
        var uid = Interlocked.Increment(ref _nextIoCommandUid);
        foreach (var listener in Listeners)
            listener.OnContextIoCommandStart(uid, kind, location, null, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageExtra);

        return uid;
    }

    public long RegisterIoCommandStartWithPath(IProcess process, IoCommandKind kind, string location, string path, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra)
    {
        var uid = Interlocked.Increment(ref _nextIoCommandUid);
        foreach (var listener in Listeners)
            listener.OnContextIoCommandStart(uid, kind, location, path, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageExtra);

        return uid;
    }

    public void RegisterIoCommandSuccess(IProcess process, IoCommandKind kind, long uid, long? affectedDataCount)
    {
        foreach (var listener in Listeners)
            listener.OnContextIoCommandEnd(process, uid, kind, affectedDataCount, null);
    }

    public void RegisterIoCommandFailed(IProcess process, IoCommandKind kind, long uid, long? affectedDataCount, Exception exception)
    {
        foreach (var listener in Listeners)
            listener.OnContextIoCommandEnd(process, uid, kind, affectedDataCount, exception);
    }

    public void RegisterWriteToSink(IReadOnlyRow row, long sinkUid)
    {
        foreach (var listener in Listeners)
            listener.OnWriteToSink(row, sinkUid);
    }

    public IRow CreateRow(IProcess process)
    {
        var row = (IRow)Activator.CreateInstance(RowType);
        row.Init(this, process, Interlocked.Increment(ref _nextRowUid), null);

        foreach (var listener in Listeners)
            listener.OnRowCreated(row);

        return row;
    }

    public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues)
    {
        var row = (IRow)Activator.CreateInstance(RowType);
        row.Init(this, process, Interlocked.Increment(ref _nextRowUid), initialValues);

        foreach (var listener in Listeners)
            listener.OnRowCreated(row);

        return row;
    }

    public IRow CreateRow(IProcess process, IReadOnlySlimRow source)
    {
        var row = (IRow)Activator.CreateInstance(RowType);
        row.Init(this, process, Interlocked.Increment(ref _nextRowUid), source.Values);
        row.Tag = source.Tag;

        foreach (var listener in Listeners)
            listener.OnRowCreated(row);

        return row;
    }

    public EtlTransactionScope BeginTransactionScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity, TimeSpan? timeoutOverride = null)
    {
        return new EtlTransactionScope(this, process, kind, timeoutOverride ?? TransactionScopeTimeout, logSeverity);
    }

    public void SetRowOwner(IRow row, IProcess currentProcess)
    {
        if (row.CurrentProcess == currentProcess)
            return;

        var previousProcess = row.CurrentProcess;
        row.CurrentProcess = currentProcess;

        foreach (var listener in Listeners)
            listener.OnRowOwnerChanged(row, previousProcess, currentProcess);
    }

    public void RegisterProcessInvocationStart(IProcess process, ICaller caller)
    {
        process.InvocationInfo = new ProcessInvocationInfo()
        {
            InstanceUid = process.InvocationInfo?.InstanceUid ?? Interlocked.Increment(ref _nextProcessInstanceUid),
            Number = (process.InvocationInfo?.Number ?? 0) + 1L,
            InvocationUid = Interlocked.Increment(ref _nextProcessInvocationUid),
            InvocationStarted = Stopwatch.StartNew(),
            Caller = caller,
        };

        foreach (var listener in Listeners)
            listener.OnProcessInvocationStart(process);
    }

    public void RegisterProcessInvocationEnd(IProcess process)
    {
        if (process.InvocationInfo.LastInvocationFinished != null)
            Debugger.Break();

        process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;

        foreach (var listener in Listeners)
            listener.OnProcessInvocationEnd(process);
    }

    public void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds)
    {
        /*if (process.InvocationInfo.LastInvocationFinished != null)
            Debugger.Break();*/

        process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;
        process.InvocationInfo.LastInvocationNetTimeMilliseconds = netElapsedMilliseconds;

        foreach (var listener in Listeners)
            listener.OnProcessInvocationEnd(process);
    }

    public long GetSinkUid(string location, string path)
    {
        var key = location + " / " + path;
        if (!_sinks.TryGetValue(key, out var sinkUid))
        {
            sinkUid = Interlocked.Increment(ref _nextSinkUid);
            _sinks.Add(key, sinkUid);
            foreach (var listener in Listeners)
                listener.OnSinkStarted(sinkUid, location, path);
        }

        return sinkUid;
    }

    public void Close()
    {
        foreach (var listener in Listeners)
        {
            listener.OnContextClosed();
            if (listener is IDisposable disp)
                disp.Dispose();
        }

        Listeners.Clear();
    }

    public void StopServices()
    {
        foreach (var service in _services)
            service.Stop();

        _services.Clear();
    }
}