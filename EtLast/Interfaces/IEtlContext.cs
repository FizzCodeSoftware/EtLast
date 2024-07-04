namespace FizzCode.EtLast;

public interface IEtlContext : ICaller
{
    public IArgumentCollection Arguments { get; }
    public T Service<T>() where T : IEtlService, new();
    public AdditionalData AdditionalData { get; }

    public void RegisterScopeAction(ScopeAction action);
    public ScopeAction[] GetScopeActions();

    public int ElapsedMillisecondsLimitToLog { get; set; }

    public TimeSpan TransactionScopeTimeout { get; set; }

    public void Terminate();
    public bool IsTerminating { get; }
    public CancellationToken CancellationToken { get; }

    public ContextManifest Manifest { get; }
    public List<IEtlContextListener> Listeners { get; }

    public IRow CreateRow(IProcess process);
    public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues);
    public IRow CreateRow(IProcess process, IReadOnlySlimRow source);

    public void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args);
    public void Log(LogSeverity severity, IProcess process, string text, params object[] args);
    public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args);

    public void LogCustom(string fileName, IProcess process, string text, params object[] args);
    public void LogCustomOps(string fileName, IProcess process, string text, params object[] args);

    public IoCommand RegisterIoCommand(IoCommand ioCommand);

    public void RegisterProcessStart(IProcess process, ICaller caller);
    public void RegisterProcessEnd(IProcess process);
    public void RegisterProcessEnd(IProcess process, long netElapsedMilliseconds);

    public Sink GetSink(string location, string path, string format, IProcess process, string[] columns);

    public void Close();
    public void StopServices();
}