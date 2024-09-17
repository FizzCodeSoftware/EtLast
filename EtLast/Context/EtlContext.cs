using System.Reflection;

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
    private long _nextSinkId;
    private long _nextIoCommandId;
    private readonly Dictionary<string, Sink> _sinksByKey = new(StringComparer.InvariantCultureIgnoreCase);
    private readonly Dictionary<string, List<Sink>> _sinksByLocationAndPath = new(StringComparer.InvariantCultureIgnoreCase);

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
            EtLastVersion = typeof(IEtlContext).Assembly.GetName().Version.ToString(),
            HostVersion = Assembly.GetEntryAssembly().GetName().Version.ToString(),
            RuntimeMajorVersion = Environment.Version.Major,
            RuntimeVersion = Environment.Version.ToString(),
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

    public void RegisterProcessStart(IProcess process, ICaller caller)
    {
        process.ExecutionInfo = new ProcessExecutionInfo()
        {
            Id = Interlocked.Increment(ref _nextProcessId),
            Timer = Stopwatch.StartNew(),
            Caller = caller,
        };

        foreach (var listener in Listeners)
            listener.OnProcessStart(process);
    }

    public void RegisterProcessEnd(IProcess process)
    {
        if (process.ExecutionInfo.FinishedOnLocal != null)
            Debugger.Break();

        process.ExecutionInfo.FinishedOnLocal = DateTimeOffset.Now;
        process.ExecutionInfo.FinishedOnUtc = process.ExecutionInfo.FinishedOnLocal.Value.ToLocalTime();

        foreach (var listener in Listeners)
            listener.OnProcessEnd(process);
    }

    public void RegisterProcessEnd(IProcess process, long netElapsedMilliseconds)
    {
        process.ExecutionInfo.FinishedOnLocal = DateTimeOffset.Now;
        process.ExecutionInfo.FinishedOnUtc = process.ExecutionInfo.FinishedOnLocal.Value.ToLocalTime();
        process.ExecutionInfo.NetTimeMilliseconds = netElapsedMilliseconds;

        foreach (var listener in Listeners)
            listener.OnProcessEnd(process);
    }

    public Sink GetSink(string location, string path, string format, IProcess process, string[] columns)
    {
        var key = location + '\0' + path + '\0' + format + '\0' + process.ExecutionInfo.Id.ToString("D", CultureInfo.InvariantCulture);
        if (!_sinksByKey.TryGetValue(key, out var sink))
        {
            _sinksByKey[key] = sink = new Sink()
            {
                Id = Interlocked.Increment(ref _nextSinkId),
                Context = this,
                Location = location,
                Path = path,
                Format = format,
                ProcessId = process.ExecutionInfo.Id,
                Columns = columns,
            };

            key = location + '\0' + path;
            if (!_sinksByLocationAndPath.TryGetValue(key, out var list))
            {
                _sinksByLocationAndPath[key] =
                [
                    sink
                ];
            }
            else
            {
                list.Add(sink);
            }

            foreach (var listener in Listeners)
                listener.OnSinkStarted(process, sink);
        }

        return sink;
    }

    public List<Sink> QuerySinks(string location, string path)
    {
        var key = location + '\0' + path;
        _sinksByLocationAndPath.TryGetValue(key, out var list);
        return list;
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