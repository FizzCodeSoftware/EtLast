namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;
using System.Threading;

public interface IEtlContext
{
    public void SetRowType<T>() where T : IRow;

    public List<Exception> Exceptions { get; }
    public int WarningCount { get; }
    public AdditionalData AdditionalData { get; }

    public string Uid { get; }
    public DateTimeOffset CreatedOnUtc { get; }
    public DateTimeOffset CreatedOnLocal { get; }

    public TimeSpan TransactionScopeTimeout { get; }
    public EtlTransactionScope BeginScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity);

    public CancellationTokenSource CancellationTokenSource { get; }

    public List<IEtlContextListener> Listeners { get; }

    public IRow CreateRow(IProcess process);
    public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues);
    public IRow CreateRow(IProcess process, IReadOnlySlimRow source);

    public void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args);
    public void Log(LogSeverity severity, IProcess process, string text, params object[] args);
    public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args);

    public void LogCustom(string fileName, IProcess process, string text, params object[] args);
    public void LogCustomOps(string fileName, IProcess process, string text, params object[] args);

    public int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs);
    public int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, string path, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs);
    public void RegisterIoCommandSuccess(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount);
    public void RegisterIoCommandFailed(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount, Exception exception);

    public void RegisterWriteToSink(IReadOnlyRow row, int sinkUid);

    public void AddException(IProcess process, Exception ex);
    public List<Exception> GetExceptions();

    public int ExceptionCount { get; }

    public void SetRowOwner(IRow row, IProcess currentProcess);

    public void RegisterProcessInvocationStart(IProcess process, IProcess caller);
    public void RegisterProcessInvocationEnd(IProcess process);
    public void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds);
    public int GetSinkUid(string location, string path);

    public void Close();
}
