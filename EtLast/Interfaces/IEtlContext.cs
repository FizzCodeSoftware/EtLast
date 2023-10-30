﻿namespace FizzCode.EtLast;

public interface IEtlContext
{
    public void SetRowType<T>() where T : IRow;

    public ArgumentCollection Arguments { get; }
    public T Service<T>() where T : IEtlService, new();
    public AdditionalData AdditionalData { get; }

    public void RegisterScopeAction(ScopeAction action);
    public ScopeAction[] GetScopeActions();

    public string Id { get; }
    public string Name { get; }
    public DateTimeOffset CreatedOnUtc { get; }
    public DateTimeOffset CreatedOnLocal { get; }

    public int ElapsedMillisecondsLimitToLog { get; set; }

    public TimeSpan TransactionScopeTimeout { get; set; }
    public EtlTransactionScope BeginTransactionScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity, TimeSpan? timeoutOverride = null);

    public void Terminate();
    public bool IsTerminating { get; }
    public CancellationToken CancellationToken { get; }

    public List<IEtlContextListener> Listeners { get; }

    public IRow CreateRow(IProcess process, bool keepNulls = false);
    public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues, bool keepNulls = false);
    public IRow CreateRow(IProcess process, IReadOnlySlimRow source);

    public void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args);
    public void Log(LogSeverity severity, IProcess process, string text, params object[] args);
    public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args);

    public void LogCustom(string fileName, IProcess process, string text, params object[] args);
    public void LogCustomOps(string fileName, IProcess process, string text, params object[] args);

    public int RegisterIoCommandStart(IProcess process, IoCommandKind kind, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra);
    public int RegisterIoCommandStartWithLocation(IProcess process, IoCommandKind kind, string location, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra);
    public int RegisterIoCommandStartWithPath(IProcess process, IoCommandKind kind, string location, string path, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, string messageExtra);
    public void RegisterIoCommandSuccess(IProcess process, IoCommandKind kind, int uid, long? affectedDataCount);
    public void RegisterIoCommandFailed(IProcess process, IoCommandKind kind, int uid, long? affectedDataCount, Exception exception);

    public void RegisterWriteToSink(IReadOnlyRow row, int sinkUid);

    public void SetRowOwner(IRow row, IProcess currentProcess);

    public void RegisterProcessInvocationStart(IProcess process, IProcess caller);
    public void RegisterProcessInvocationEnd(IProcess process);
    public void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds);
    public int GetSinkUid(string location, string path);

    public void Close();
    public void StopServices();
}
