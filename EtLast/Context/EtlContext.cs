namespace FizzCode.EtLast;

public sealed class EtlContext : IEtlContext
{
    public AdditionalData AdditionalData { get; } = new AdditionalData();
    public IArgumentCollection Arguments { get; }

    public IEtlContext Context => this;
    public FlowState FlowState => new(this);

    public List<IEtlContextListener> Listeners { get; }

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

    private long _nextRowId;
    private long _nextProcessId;
    private long _nextInvocationId;
    private long _nextSinkId;
    private long _nextIoCommandId;
    private readonly Dictionary<string, Sink> _sinks = [];

    private readonly List<ScopeAction> _scopeActions = [];

    public ContextManifest Manifest { get; }

    public EtlContext(IArgumentCollection arguments, string customName = null, string commandId = null)
    {
        _cancellationTokenSource = new CancellationTokenSource();
        CancellationToken = _cancellationTokenSource.Token;

        Arguments = arguments;

        var nowUtc = DateTimeOffset.UtcNow;

        Manifest = new ContextManifest()
        {
            ContextId = long.Parse(DateTime.UtcNow.ToString("yyyyMMddHHmmssfffff", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture),
            CommandId = commandId,
            ContextName = customName ?? Guid.NewGuid().ToString("D"),
            Instance = arguments?.Instance ?? Environment.MachineName,
            UserName = Environment.UserName,
            UserDomainName = Environment.UserDomainName,
            OSVersion = Environment.OSVersion.ToString(),
            ProcessorCount = Environment.ProcessorCount,
            UserInteractive = Environment.UserInteractive,
            Is64Bit = Environment.Is64BitProcess,
            IsPrivileged = Environment.IsPrivilegedProcess,
            TickCountSinceStartup = Environment.TickCount64,
            CreatedOnUtc = nowUtc,
            CreatedOnLocal = nowUtc.ToLocalTime(),
        };

        Listeners = [Manifest];
    }

    public void RegisterScopeAction(ScopeAction action)
    {
        _scopeActions.Add(action);
    }

    public ScopeAction[] GetScopeActions()
    {
        return [.. _scopeActions];
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

    public IoCommand RegisterIoCommand(IoCommand ioCommand)
    {
        ioCommand.Id = Interlocked.Increment(ref _nextIoCommandId);
        ioCommand.Context = this;

        foreach (var listener in Listeners)
            listener.OnContextIoCommandStart(ioCommand);

        return ioCommand;
    }

    public IRow CreateRow(IProcess process)
    {
        var row = new Row(this, process, Interlocked.Increment(ref _nextRowId), null);

        foreach (var listener in Listeners)
            listener.OnRowCreated(row);

        return row;
    }

    public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues)
    {
        var row = new Row(this, process, Interlocked.Increment(ref _nextRowId), initialValues);

        foreach (var listener in Listeners)
            listener.OnRowCreated(row);

        return row;
    }

    public IRow CreateRow(IProcess process, IReadOnlySlimRow source)
    {
        var row = new Row(this, process, Interlocked.Increment(ref _nextRowId), source.Values)
        {
            Tag = source.Tag
        };

        foreach (var listener in Listeners)
            listener.OnRowCreated(row);

        return row;
    }

    public void RegisterProcessInvocationStart(IProcess process, ICaller caller)
    {
        process.InvocationInfo = new ProcessInvocationInfo()
        {
            ProcessId = process.InvocationInfo?.ProcessId ?? Interlocked.Increment(ref _nextProcessId),
            ProcessInvocationCount = (process.InvocationInfo?.ProcessInvocationCount ?? 0) + 1L,
            InvocationId = Interlocked.Increment(ref _nextInvocationId),
            InvocationStarted = Stopwatch.StartNew(),
            Caller = caller,
        };

        foreach (var listener in Listeners)
            listener.OnProcessInvocationStart(process);
    }

    public void RegisterProcessInvocationEnd(IProcess process)
    {
        if (process.InvocationInfo.LastInvocationFinishedLocal != null)
            Debugger.Break();

        process.InvocationInfo.LastInvocationFinishedLocal = DateTimeOffset.Now;
        process.InvocationInfo.LastInvocationFinishedUtc = process.InvocationInfo.LastInvocationFinishedLocal.Value.ToLocalTime();

        foreach (var listener in Listeners)
            listener.OnProcessInvocationEnd(process);
    }

    public void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds)
    {
        process.InvocationInfo.LastInvocationFinishedLocal = DateTimeOffset.Now;
        process.InvocationInfo.LastInvocationFinishedUtc = process.InvocationInfo.LastInvocationFinishedLocal.Value.ToLocalTime();
        process.InvocationInfo.LastInvocationNetTimeMilliseconds = netElapsedMilliseconds;

        foreach (var listener in Listeners)
            listener.OnProcessInvocationEnd(process);
    }

    public Sink GetSink(string location, string path, string format, IProcess process, string[] columns)
    {
        var key = location + " / " + path + " / " + format + " / " + process.InvocationInfo.InvocationId.ToString("D", CultureInfo.InvariantCulture);
        if (!_sinks.TryGetValue(key, out var sink))
        {
            _sinks[key] = sink = new Sink()
            {
                Id = Interlocked.Increment(ref _nextSinkId),
                Context = this,
                Location = location,
                Path = path,
                Format = format,
                ProcessInvocationId = process.InvocationInfo.InvocationId,
                Columns = columns,
            };

            foreach (var listener in Listeners)
                listener.OnSinkStarted(process, sink);
        }

        return sink;
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