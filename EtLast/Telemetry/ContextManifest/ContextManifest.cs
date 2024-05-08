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
            ProcessId = process.InvocationInfo.ProcessId,
            ProcessName = process.Name,
            ProcessTypeName = process.GetType().GetFriendlyTypeName(),
            Message = exception.Message,
            Details = exception.FormatExceptionWithDetails(true),
        };

        AllExceptions.Add(manifestException);

        ManifestExceptionAdded?.Invoke(this, manifestException);
        ManifestChanged?.Invoke(this);

        _exceptionMap[exception] = manifestException;
    }

    public void OnSinkStarted(IProcess process, Sink sink)
    {
        var manifestSink = new ContextManifestSink()
        {
            Id = sink.Id,
            Location = sink.Location,
            Path = sink.Path,
            Format = sink.Format,
            ProcessInvocationId = process.InvocationInfo.InvocationId,
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

    public void OnProcessInvocationStart(IProcess process)
    {
        if (process.InvocationInfo.Caller is IEtlContext)
        {
            if (!_processes.TryGetValue(process.InvocationInfo.ProcessId, out var manifestProcess))
            {
                manifestProcess = new ContextManifestProcess()
                {
                    ProcessId = process.InvocationInfo.ProcessId,
                    Name = process.Name,
                    TypeName = process.GetType().GetFriendlyTypeName(),
                    Kind = process.Kind,
                };

                _processes[manifestProcess.ProcessId] = manifestProcess;
            }

            var manifestInvocation = new ContextManifestProcessInvocation()
            {
                StartedOnUtc = DateTimeOffset.UtcNow,
                InvocationId = process.InvocationInfo.InvocationId,
                ProcessInvocationCount = process.InvocationInfo.ProcessInvocationCount,
            };

            manifestProcess.Invocations.Add(manifestInvocation);

            ManifestProcessStarted?.Invoke(this, manifestProcess);
            ManifestChanged?.Invoke(this);
        }
    }

    public void OnProcessInvocationEnd(IProcess process)
    {
        if (_processes.TryGetValue(process.InvocationInfo.InvocationId, out var manifestProcess))
        {
            var manifestInvocation = manifestProcess.Invocations[(int)process.InvocationInfo.ProcessInvocationCount - 1];

            manifestInvocation.FinishedOnUtc = DateTimeOffset.UtcNow;
            manifestInvocation.Success = !process.FlowState.Failed;

            if (process.FlowState.Failed)
                AnyRootProcessFailed = true;

            manifestInvocation.FailureExceptions.AddRange(process.FlowState.Exceptions.Select(ex =>
            {
                _exceptionMap.TryGetValue(ex, out var manifestException);
                return manifestException;
            }).Where(x => x != null));

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
    public void OnRowCreated(IReadOnlyRow row) { }
    public void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess) { }
    public void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values) { }
}

public class ContextManifestSink
{
    public long Id { get; set; }
    public string Location { get; set; }
    public string Path { get; set; }
    public string Format { get; set; }
    public long ProcessInvocationId { get; set; }
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
    public long ProcessId { get; set; }
    public string ProcessName { get; set; }
    public string ProcessTypeName { get; set; }

    public string Message { get; set; }
    public string Details { get; set; }
}

public class ContextManifestProcess
{
    public long ProcessId { get; set; }
    public string Name { get; set; }
    public string TypeName { get; set; }
    public string Kind { get; set; }

    public List<ContextManifestProcessInvocation> Invocations { get; } = [];
}

public class ContextManifestProcessInvocation
{
    public DateTimeOffset StartedOnUtc { get; set; }
    public long InvocationId { get; set; }
    public long ProcessInvocationCount { get; set; }

    public DateTimeOffset? FinishedOnUtc { get; set; }
    public bool? Success { get; set; }
    public List<ContextManifestException> FailureExceptions { get; } = [];
}