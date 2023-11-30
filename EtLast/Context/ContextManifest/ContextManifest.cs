namespace FizzCode.EtLast;

public delegate void ContextManifestChangedEvent(ContextManifest manifest);

public delegate void ContextManifestClosedEvent(ContextManifest manifest);
public delegate void ContextManifestTaskStartedEvent(ContextManifest manifest, ContextManifestProcess task);
public delegate void ContextManifestTaskFinishedEvent(ContextManifest manifest, ContextManifestProcess task);
public delegate void ContextManifestSinkCreatedEvent(ContextManifest manifest, ContextManifestSink sink);
public delegate void ContextManifestSinkChangedEvent(ContextManifest manifest, ContextManifestSink sink);
public delegate void ContextManifestExceptionAddedEvent(ContextManifest manifest, ContextManifestException exception);
public delegate void ContextManifestIoTargetCreatedEvent(ContextManifest manifest, ContextManifestIoTarget target);
public delegate void ContextManifestIoTargetChangedEvent(ContextManifest manifest, ContextManifestIoTarget target);

public class ContextManifest : IEtlContextListener
{
    public long ContextId { get; set; }
    public string ContextName { get; set; }
    public string Instance { get; set; }
    public string UserName { get; set; }
    public string UserDomainName { get; set; }
    public string OSVersion { get; set; }
    public int ProcessorCount { get; set; }
    public bool UserInteractive { get; set; }
    public bool Is64Bit { get; set; }
    public bool IsPrivileged { get; set; }
    public long TickCount { get; set; }
    public DateTimeOffset CreatedOnUtc { get; set; }
    public DateTimeOffset CreatedOnLocal { get; set; }
    public Dictionary<string, string> Arguments { get; set; }

    public IReadOnlyList<ContextManifestSink> Sinks
    {
        get => _sinks.Values.ToList();
        set { _sinks = value.ToDictionary(x => x.Id, x => x); }
    }

    public IReadOnlyList<ContextManifestProcess> TopLevelTasks
    {
        get => _tasks.Values.ToList();
        set { _tasks = value.ToDictionary(x => x.ProcessId, x => x); }
    }

    public DateTimeOffset? ClosedOnUtc { get; private set; }

    public List<ContextManifestException> AllExceptions { get; } = [];

    public IReadOnlyList<ContextManifestIoTarget> IoTargets
    {
        get => _ioTargets.Values.ToList();
        set { _ioTargets = value.ToDictionary(x => (x.Location, x.Path, x.Kind), x => x); }
    }

    public event ContextManifestChangedEvent ManifestChanged;
    public event ContextManifestClosedEvent ManifestClosed;
    public event ContextManifestTaskStartedEvent ManifestTaskStarted;
    public event ContextManifestTaskFinishedEvent ManifestTaskFinished;
    public event ContextManifestSinkCreatedEvent ManifestSinkCreated;
    public event ContextManifestSinkChangedEvent ManifestSinkChanged;
    public event ContextManifestExceptionAddedEvent ManifestExceptionAdded;
    public event ContextManifestIoTargetCreatedEvent ManifestIoTargetCreated;
    public event ContextManifestIoTargetChangedEvent ManifestIoTargetChanged;

    private Dictionary<long, ContextManifestSink> _sinks = [];
    private Dictionary<long, ContextManifestProcess> _tasks = [];
    private Dictionary<(string, string, string), ContextManifestIoTarget> _ioTargets = [];
    private readonly Dictionary<long, ContextManifestIoTarget> _ioCommands = [];

    private readonly Dictionary<Exception, ContextManifestException> _exceptionMap = [];

    public void Start()
    {
    }

    public void OnContextClosed()
    {
        ClosedOnUtc = DateTimeOffset.UtcNow;
        ManifestClosed?.Invoke(this);
        ManifestChanged?.Invoke(this);
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

    public void OnSinkStarted(long sinkId, string location, string path, string sinkFormat, Type sinkWriter)
    {
        var sink = new ContextManifestSink()
        {
            Id = sinkId,
            Location = location,
            Path = path,
            Format = sinkFormat,
            WriterType = sinkWriter.GetFriendlyTypeName(),
        };

        _sinks[sinkId] = sink;

        ManifestSinkCreated?.Invoke(this, sink);
        ManifestChanged?.Invoke(this);
    }

    public void OnWriteToSink(IReadOnlyRow row, long sinkId)
    {
        if (_sinks.TryGetValue(sinkId, out var sink))
        {
            sink.RowsWritten++;

            ManifestSinkChanged?.Invoke(this, sink);
            ManifestChanged?.Invoke(this);
        }
    }

    public void OnProcessInvocationStart(IProcess process)
    {
        if (process is IEtlTask task && process.InvocationInfo.Caller is IEtlContext)
        {
            if (!_tasks.TryGetValue(task.InvocationInfo.ProcessId, out var manifestTask))

            {
                manifestTask = new ContextManifestProcess()
                {
                    ProcessId = task.InvocationInfo.ProcessId,
                    Name = task.Name,
                    TypeName = task.GetType().GetFriendlyTypeName(),
                };

                _tasks[manifestTask.ProcessId] = manifestTask;
            }

            var manifestInvocation = new ContextManifestProcessInvocation()
            {
                StartedOnUtc = DateTimeOffset.UtcNow,
                InvocationId = task.InvocationInfo.InvocationId,
                ProcessInvocationCount = task.InvocationInfo.ProcessInvocationCount,
            };

            manifestTask.Invocations.Add(manifestInvocation);

            ManifestTaskStarted?.Invoke(this, manifestTask);
            ManifestChanged?.Invoke(this);
        }
    }

    public void OnProcessInvocationEnd(IProcess process)
    {
        if (_tasks.TryGetValue(process.InvocationInfo.InvocationId, out var manifestTask))
        {
            var manifestInvocation = manifestTask.Invocations[(int)process.InvocationInfo.ProcessInvocationCount - 1];

            manifestInvocation.FinishedOnUtc = DateTimeOffset.UtcNow;
            manifestInvocation.Success = !process.FlowState.Failed;

            manifestInvocation.FailureExceptions.AddRange(process.FlowState.Exceptions.Select(ex =>
            {
                _exceptionMap.TryGetValue(ex, out var manifestException);
                return manifestException;
            }).Where(x => x != null));

            ManifestTaskFinished?.Invoke(this, manifestTask);
            ManifestChanged?.Invoke(this);
        }
    }

    public void OnContextIoCommandStart(long id, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra)
    {
        if (location == null)
            return;

        var key = (location, path, kind.ToString());
        if (!_ioTargets.TryGetValue(key, out var target))
        {
            _ioTargets[key] = target = new ContextManifestIoTarget()
            {
                Location = location,
                Path = path,
                Kind = key.Item3,
            };

            ManifestIoTargetCreated?.Invoke(this, target);
            ManifestChanged?.Invoke(this);
        }

        _ioCommands[id] = target;
    }

    public void OnContextIoCommandEnd(IProcess process, long id, IoCommandKind kind, long? affectedDataCount, Exception ex)
    {
        if (_ioCommands.TryGetValue(id, out var target))
        {
            if (affectedDataCount != null)
                target.AffectedDataCount += affectedDataCount.Value;

            target.CommandCount++;
            if (ex != null)
                target.ErrorCount++;

            ManifestIoTargetChanged?.Invoke(this, target);
            ManifestChanged?.Invoke(this);
        }
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
    public string Format { get; set; }
    public string WriterType { get; set; }
    public string Location { get; set; }
    public string Path { get; set; }

    public long RowsWritten { get; set; }
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