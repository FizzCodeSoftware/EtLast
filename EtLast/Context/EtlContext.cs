namespace FizzCode.EtLast;

public sealed class EtlContext : IEtlContext
{
    public Type RowType { get; private set; }

    public int WarningCount { get; internal set; }
    public AdditionalData AdditionalData { get; }

    public string Uid { get; }
    public DateTimeOffset CreatedOnUtc { get; }
    public DateTimeOffset CreatedOnLocal { get; }

    public List<IEtlContextListener> Listeners { get; } = new List<IEtlContextListener>();

    /// <summary>
    /// Default value: 4 hours, but .NET maximizes the timeout in 10 minutes.
    /// </summary>
    public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromHours(4);

    private readonly CancellationTokenSource _cancellationTokenSource;
    public CancellationToken CancellationToken { get; }

    private int _nextRowUid;
    private int _nextProcessInstanceUid;
    private int _nextProcessInvocationUid;
    private int _nextSinkUid;
    private int _nextIoCommandUid;
    private readonly List<Exception> _exceptions = new();
    private readonly Dictionary<string, int> _sinks = new();

    public EtlContext()
    {
        SetRowType<Row>();
        _cancellationTokenSource = new CancellationTokenSource();
        CancellationToken = _cancellationTokenSource.Token;

        AdditionalData = new AdditionalData();

        Uid = Guid.NewGuid().ToString("D");
        CreatedOnLocal = DateTimeOffset.Now;
        CreatedOnUtc = CreatedOnLocal.ToUniversalTime();
    }

    public void SetRowType<T>() where T : IRow
    {
        RowType = typeof(T);
    }

    public void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args)
    {
        if (severity is LogSeverity.Error or LogSeverity.Warning)
            WarningCount++;

        foreach (var listener in Listeners)
        {
            listener.OnLog(severity, false, transactionId, process, text, args);
        }
    }

    public void Log(LogSeverity severity, IProcess process, string text, params object[] args)
    {
        if (severity is LogSeverity.Error or LogSeverity.Warning)
            WarningCount++;

        foreach (var listener in Listeners)
        {
            listener.OnLog(severity, false, null, process, text, args);
        }
    }

    public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
        {
            listener.OnLog(severity, true, null, process, text, args);
        }
    }

    public void LogCustom(string fileName, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
        {
            listener.OnCustomLog(false, fileName, process, text, args);
        }
    }

    public void LogCustomOps(string fileName, IProcess process, string text, params object[] args)
    {
        foreach (var listener in Listeners)
        {
            listener.OnCustomLog(true, fileName, process, text, args);
        }
    }

    public int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
    {
        var uid = Interlocked.Increment(ref _nextIoCommandUid);
        foreach (var listener in Listeners)
        {
            listener.OnContextIoCommandStart(uid, kind, location, null, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageArgs);
        }

        return uid;
    }

    public int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, string path, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
    {
        var uid = Interlocked.Increment(ref _nextIoCommandUid);
        foreach (var listener in Listeners)
        {
            listener.OnContextIoCommandStart(uid, kind, location, path, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageArgs);
        }

        return uid;
    }

    public void RegisterIoCommandSuccess(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount)
    {
        foreach (var listener in Listeners)
        {
            listener.OnContextIoCommandEnd(process, uid, kind, affectedDataCount, null);
        }
    }

    public void RegisterIoCommandFailed(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount, Exception exception)
    {
        foreach (var listener in Listeners)
        {
            listener.OnContextIoCommandEnd(process, uid, kind, affectedDataCount, exception);
        }
    }

    public void RegisterWriteToSink(IReadOnlyRow row, int sinkUid)
    {
        foreach (var listener in Listeners)
        {
            listener.OnWriteToSink(row, sinkUid);
        }
    }

    public IRow CreateRow(IProcess process)
    {
        var row = (IRow)Activator.CreateInstance(RowType);
        row.Init(this, process, Interlocked.Increment(ref _nextRowUid), null);

        foreach (var listener in Listeners)
        {
            listener.OnRowCreated(row);
        }

        return row;
    }

    public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues)
    {
        var row = (IRow)Activator.CreateInstance(RowType);
        row.Init(this, process, Interlocked.Increment(ref _nextRowUid), initialValues);

        foreach (var listener in Listeners)
        {
            listener.OnRowCreated(row);
        }

        return row;
    }

    public IRow CreateRow(IProcess process, IReadOnlySlimRow source)
    {
        var row = (IRow)Activator.CreateInstance(RowType);
        row.Init(this, process, Interlocked.Increment(ref _nextRowUid), source.Values);
        row.Tag = source.Tag;

        foreach (var listener in Listeners)
        {
            listener.OnRowCreated(row);
        }

        return row;
    }

    public void AddException(IProcess process, Exception ex)
    {
        lock (_exceptions)
        {
            if (_exceptions.Contains(ex))
            {
                _cancellationTokenSource.Cancel();
                return;
            }

            _exceptions.Add(ex);
        }

        foreach (var listener in Listeners)
        {
            listener.OnException(process, ex);
        }

        _cancellationTokenSource.Cancel();
    }

    public List<Exception> GetExceptions()
    {
        lock (_exceptions)
        {
            return new List<Exception>(_exceptions);
        }
    }

    public int ExceptionCount
    {
        get
        {
            lock (_exceptions)
            {
                return _exceptions.Count;
            }
        }
    }

    public EtlTransactionScope BeginScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity)
    {
        return new EtlTransactionScope(this, process, kind, TransactionScopeTimeout, logSeverity);
    }

    public void SetRowOwner(IRow row, IProcess currentProcess)
    {
        if (row.CurrentProcess == currentProcess)
            return;

        var previousProcess = row.CurrentProcess;
        row.CurrentProcess = currentProcess;

        foreach (var listener in Listeners)
        {
            listener.OnRowOwnerChanged(row, previousProcess, currentProcess);
        }
    }

    public void RegisterProcessInvocationStart(IProcess process, IProcess caller)
    {
        process.InvocationInfo = new ProcessInvocationInfo()
        {
            InstanceUid = process.InvocationInfo?.InstanceUid ?? Interlocked.Increment(ref _nextProcessInstanceUid),
            Number = (process.InvocationInfo?.Number ?? 0) + 1,
            InvocationUid = Interlocked.Increment(ref _nextProcessInvocationUid),
            LastInvocationStarted = Stopwatch.StartNew(),
            Caller = caller,
        };

        foreach (var listener in Listeners)
        {
            listener.OnProcessInvocationStart(process);
        }
    }

    public void RegisterProcessInvocationEnd(IProcess process)
    {
        process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;

        foreach (var listener in Listeners)
        {
            listener.OnProcessInvocationEnd(process);
        }
    }

    public void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds)
    {
        process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;
        process.InvocationInfo.LastInvocationNetTimeMilliseconds = netElapsedMilliseconds;

        foreach (var listener in Listeners)
        {
            listener.OnProcessInvocationEnd(process);
        }
    }

    public int GetSinkUid(string location, string path)
    {
        var key = location + " / " + path;
        if (!_sinks.TryGetValue(key, out var sinkUid))
        {
            sinkUid = Interlocked.Increment(ref _nextSinkUid);
            _sinks.Add(key, sinkUid);
            foreach (var listener in Listeners)
            {
                listener.OnSinkStarted(sinkUid, location, path);
            }
        }

        return sinkUid;
    }

    public void Close()
    {
        foreach (var listener in Listeners)
        {
            listener.OnContextClosed();
            if (listener is IDisposable disp)
            {
                disp.Dispose();
            }
        }

        Listeners.Clear();
    }
}