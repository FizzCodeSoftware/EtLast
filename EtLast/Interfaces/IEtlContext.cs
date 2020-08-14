namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public interface IEtlContext
    {
        void SetRowType<T>() where T : IRow;

        EtlContextResult Result { get; }
        AdditionalData AdditionalData { get; }

        DateTimeOffset CreatedOnUtc { get; }
        DateTimeOffset CreatedOnLocal { get; }

        TimeSpan TransactionScopeTimeout { get; }
        EtlTransactionScope BeginScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity);

        CancellationTokenSource CancellationTokenSource { get; }

        List<IEtlContextListener> Listeners { get; }

        void ExecuteOne(bool terminateHostOnFail, IExecutable executable);
        void ExecuteSequence(bool terminateHostOnFail, params IExecutable[] executables);

        IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues);
        IRow CreateRow(IProcess process, IReadOnlySlimRow initialValues);

        void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args);
        void Log(LogSeverity severity, IProcess process, string text, params object[] args);
        void LogOps(LogSeverity severity, IProcess process, string text, params object[] args);

        void LogCustom(string fileName, IProcess process, string text, params object[] args);
        void LogCustomOps(string fileName, IProcess process, string text, params object[] args);

        int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs);
        int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, string path, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs);
        void RegisterIoCommandSuccess(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount);
        void RegisterIoCommandFailed(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount, Exception exception);

        void RegisterRowStored(IProcess process, IReadOnlyRow row, int storeUid);

        void AddException(IProcess process, Exception ex);
        List<Exception> GetExceptions();

        int ExceptionCount { get; }

        void SetRowOwner(IRow row, IProcess currentProcess);

        void RegisterProcessInvocationStart(IProcess process, IProcess caller);
        void RegisterProcessInvocationEnd(IProcess process);
        void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds);
        int GetStoreUid(string location, string path);

        void Close();
    }
}