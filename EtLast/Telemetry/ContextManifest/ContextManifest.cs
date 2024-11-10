namespace FizzCode.EtLast;

public delegate void ContextManifestChangedEvent(ContextManifest manifest);

public delegate void ContextManifestClosedEvent(ContextManifest manifest);
public delegate void ContextManifestProcessStartedEvent(ContextManifest manifest, ContextManifestProcess task);
public delegate void ContextManifestProcessFinishedEvent(ContextManifest manifest, ContextManifestProcess task);
public delegate void ContextManifestSinkCreatedEvent(ContextManifest manifest, ContextManifestSink sink);
public delegate void ContextManifestSinkChangedEvent(ContextManifest manifest, ContextManifestSink sink);
public delegate void ContextManifestExceptionAddedEvent(ContextManifest manifest, ContextManifestException exception);
public delegate void ContextManifestIoTargetCreatedEvent(ContextManifest manifest, ContextManifestIoTarget target);
public delegate void ContextManifestIoTargetChangedEvent(ContextManifest manifest, ContextManifestIoTarget target);

public class ContextManifest : IEtlContextListener
{
    public long ContextId { get; init; }
    public string CommandId { get; init; }
    public string ContextName { get; init; }
    public string Instance { get; init; }
    public string EtLastVersion { get; init; }
    public string HostVersion { get; init; }
    public int RuntimeMajorVersion { get; init; }
    public string RuntimeVersion { get; init; }
    public string UserName { get; init; }
    public string UserDomainName { get; init; }
    public string OSVersion { get; init; }
    public int ProcessorCount { get; init; }
    public bool UserInteractive { get; init; }
    public bool Is64Bit { get; init; }
    public bool IsPrivileged { get; init; }
    public long TickCountSinceStartup { get; init; }
    public DateTimeOffset CreatedOnUtc { get; init; }
    public DateTimeOffset CreatedOnLocal { get; init; }
    public DateTimeOffset? ClosedOnUtc { get; private set; }
    public DateTimeOffset? ClosedOnLocal { get; private set; }
    public long RamUseInitial { get; init; }
    public long RamUsePeak { get; set; }
    public long RamUse { get; set; }
    public long? RunMilliseconds { get; private set; }

    public Dictionary<string, object> Extra { get; init; } = [];

    public long TotalSinkRowsWritten { get; set; }
    public IReadOnlyList<ContextManifestSink> Sinks
    {
        get => _sinks.Values.ToList();
        set { _sinks = value.ToDictionary(x => x.Id, x => x); }
    }

    public bool AnyRootProcessFailed { get; set; }
    public IReadOnlyList<ContextManifestProcess> RootProcesses
    {
        get => _processes.Values.ToList();
        set { _processes = value.ToDictionary(x => x.ProcessId, x => x); }
    }

    public List<ContextManifestException> AllExceptions { get; } = [];

    public IReadOnlyList<ContextManifestIoTarget> IoTargets
    {
        get => _ioTargets.Values.ToList();
        set { _ioTargets = value.ToDictionary(x => (x.Location, x.Path, x.Kind), x => x); }
    }

    public event ContextManifestProcessStartedEvent ManifestProcessStarted;
    public event ContextManifestProcessFinishedEvent ManifestProcessFinished;
    public event ContextManifestSinkCreatedEvent ManifestSinkCreated;
    public event ContextManifestSinkChangedEvent ManifestSinkChanged;
    public event ContextManifestExceptionAddedEvent ManifestExceptionAdded;
    public event ContextManifestIoTargetCreatedEvent ManifestIoTargetCreated;
    public event ContextManifestIoTargetChangedEvent ManifestIoTargetChanged;

    public event ContextManifestChangedEvent ManifestChanged;

    public event ContextManifestClosedEvent ManifestClosed;

    // todo: change to ConcurrentDictionary
    private Dictionary<long, ContextManifestSink> _sinks = [];
    private Dictionary<long, ContextManifestProcess> _processes = [];
    private Dictionary<(string, string, string), ContextManifestIoTarget> _ioTargets = [];

    private readonly Dictionary<Exception, ContextManifestException> _exceptionMap = [];

    public void Start()
    {
    }

    public void OnContextClosed()
    {
        ClosedOnUtc = DateTimeOffset.UtcNow;
        ClosedOnLocal = ClosedOnUtc.Value.ToLocalTime();
        RunMilliseconds = Convert.ToInt64(ClosedOnUtc.Value.Subtract(CreatedOnUtc).TotalMilliseconds);
        ManifestClosed?.Invoke(this);
    }

    public void OnException(IProcess process, Exception exception)
    {
        var manifestException = new ContextManifestException()
        {
            ProcessId = process.ExecutionInfo.Id,
            ProcessName = process.Name,
            ProcessTypeName = process.GetType().GetFriendlyTypeName(),
            ProcessKind = process.Kind,
            Message = exception.Message,
            Details = exception.FormatExceptionWithDetails(true),
        };

        AllExceptions.Add(manifestException);

        ManifestExceptionAdded?.Invoke(this, manifestException);
        ManifestChanged?.Invoke(this);

        _exceptionMap[exception] = manifestException;
    }

    public ContextManifestException[] GetFlowStateExceptions(IReadOnlyFlowState flowState)
    {
        return flowState.Exceptions
            .Select(ex =>
            {
                _exceptionMap.TryGetValue(ex, out var manifestException);
                return manifestException;
            })
            .Where(x => x != null)
            .ToArray();
    }

    public void OnSinkStarted(IProcess process, Sink sink)
    {
        var manifestSink = new ContextManifestSink()
        {
            Id = sink.Id,
            Location = sink.Location,
            Path = sink.Path,
            Format = sink.Format,
            ProcessId = process.ExecutionInfo.Id,
            ProcessType = process.GetType().GetFriendlyTypeName(),
            Columns = sink.Columns != null ? [.. sink.Columns] : [],
        };

        _sinks[sink.Id] = manifestSink;

        ManifestSinkCreated?.Invoke(this, manifestSink);
        ManifestChanged?.Invoke(this);
    }

    public void OnWriteToSink(Sink sink, IReadOnlyRow row)
    {
        if (_sinks.TryGetValue(sink.Id, out var manifestSink))
        {
            manifestSink.RowsWritten++;
            TotalSinkRowsWritten++;

            ManifestSinkChanged?.Invoke(this, manifestSink);
            ManifestChanged?.Invoke(this);
        }
    }

    public void OnProcessStart(IProcess process)
    {
        if (process.ExecutionInfo.Caller is IEtlContext)
        {
            var manifestProcess = new ContextManifestProcess()
            {
                ProcessId = process.ExecutionInfo.Id,
                Name = process.Name,
                TypeName = process.GetType().GetFriendlyTypeName(),
                Kind = process.Kind,
                StartedOnUtc = DateTimeOffset.UtcNow,
            };

            _processes[manifestProcess.ProcessId] = manifestProcess;

            ManifestProcessStarted?.Invoke(this, manifestProcess);
            ManifestChanged?.Invoke(this);
        }
    }

    public void OnProcessEnd(IProcess process)
    {
        if (_processes.TryGetValue(process.ExecutionInfo.Id, out var manifestProcess))
        {
            manifestProcess.FinishedOnUtc = DateTimeOffset.UtcNow;
            manifestProcess.Success = !process.FlowState.Failed;

            if (process.FlowState.Failed)
                AnyRootProcessFailed = true;

            manifestProcess.FailureExceptions.AddRange(GetFlowStateExceptions(process.FlowState));

            ManifestProcessFinished?.Invoke(this, manifestProcess);
            ManifestChanged?.Invoke(this);
        }
    }

    public void OnContextIoCommandStart(IoCommand ioCommand)
    {
    }

    public void OnContextIoCommandEnd(IoCommand ioCommand)
    {
        if (ioCommand.Location == null)
            return;

        var key = (ioCommand.Location, ioCommand.Path, ioCommand.Kind.ToString());
        if (!_ioTargets.TryGetValue(key, out var target))
        {
            _ioTargets[key] = target = new ContextManifestIoTarget()
            {
                Location = ioCommand.Location,
                Path = ioCommand.Path,
                Kind = key.Item3,
            };

            ManifestIoTargetCreated?.Invoke(this, target);
            ManifestChanged?.Invoke(this);
        }

        if (ioCommand.AffectedDataCount != null)
            target.AffectedDataCount += ioCommand.AffectedDataCount.Value;

        target.CommandCount++;
        if (ioCommand.Exception != null)
            target.ErrorCount++;

        ManifestIoTargetChanged?.Invoke(this, target);
        ManifestChanged?.Invoke(this);
    }

    public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args) { }
    public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args) { }
}

public class ContextManifestSink
{
    public long Id { get; set; }
    public string Location { get; set; }
    public string Path { get; set; }
    public string Format { get; set; }
    public long ProcessId { get; set; }
    public string ProcessType { get; set; }

    public long RowsWritten { get; set; }
    public List<string> Columns { get; set; }
}

public class ContextManifestIoTarget
{
    public string Location { get; set; }
    public string Path { get; set; }
    public string Kind { get; set; }

    public int CommandCount { get; set; }
    public int ErrorCount { get; set; }
    public long AffectedDataCount { get; set; }
}

public class ContextManifestException
{
    public required long ProcessId { get; init; }
    public required string ProcessName { get; init; }
    public required string ProcessTypeName { get; init; }
    public required string ProcessKind { get; init; }
    public required string Message { get; init; }
    public required string Details { get; init; }
}

public class ContextManifestProcess
{
    public required long ProcessId { get; init; }
    public required string Name { get; init; }
    public required string TypeName { get; init; }
    public required string Kind { get; init; }

    public DateTimeOffset StartedOnUtc { get; set; }
    public DateTimeOffset? FinishedOnUtc { get; set; }
    public bool? Success { get; set; }
    public List<ContextManifestException> FailureExceptions { get; } = [];
}