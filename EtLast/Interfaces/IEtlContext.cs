namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public interface IEtlContext
    {
        void SetRowType<T>() where T : IRow;

        StatCounterCollection CounterCollection { get; }
        EtlContextResult Result { get; }
        AdditionalData AdditionalData { get; }

        DateTimeOffset CreatedOnUtc { get; }
        DateTimeOffset CreatedOnLocal { get; }

        TimeSpan TransactionScopeTimeout { get; }
        EtlTransactionScope BeginScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity);

        CancellationTokenSource CancellationTokenSource { get; }

        void ExecuteOne(bool terminateHostOnFail, IExecutable executable);
        void ExecuteSequence(bool terminateHostOnFail, params IExecutable[] executables);

        IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues);

        void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args);
        void Log(LogSeverity severity, IProcess process, string text, params object[] args);
        void LogOps(LogSeverity severity, IProcess process, string text, params object[] args);

        void LogNoDiag(LogSeverity severity, IProcess process, string text, params object[] args);

        void LogCustom(string fileName, IProcess process, string text, params object[] args);
        void LogCustomOps(string fileName, IProcess process, string text, params object[] args);

        int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string target, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs);
        void RegisterIoCommandSuccess(IProcess process, int uid, int affectedDataCount);
        void RegisterIoCommandFailed(IProcess process, int uid, int affectedDataCount, Exception exception);

        void AddException(IProcess process, Exception ex);
        List<Exception> GetExceptions();

        int ExceptionCount { get; }

        void SetRowOwner(IRow row, IProcess currentProcess);

        EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        ContextOnLogDelegate OnLog { get; set; }
        ContextOnCustomLogDelegate OnCustomLog { get; set; }

        ContextOnRowCreatedDelegate OnRowCreated { get; set; }
        ContextOnRowOwnerChangedDelegate OnRowOwnerChanged { get; set; }
        ContextOnRowValueChangedDelegate OnRowValueChanged { get; set; }
        ContextOnRowStoreStartedDelegate OnRowStoreStarted { get; set; }
        ContextOnRowStoredDelegate OnRowStored { get; set; }
        public ContextOnProcessInvocationDelegate OnProcessInvocationStart { get; set; }
        public ContextOnProcessInvocationDelegate OnProcessInvocationEnd { get; set; }
        public ContextOnIoCommandStartDelegate OnContextIoCommandStart { get; set; }
        public ContextOnIoCommandEndDelegate OnContextIoCommandEnd { get; set; }

        void RegisterProcessInvocationStart(IProcess process, IProcess caller);
        void RegisterProcessInvocationEnd(IProcess process);
        void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds);
        int GetStoreUid(List<KeyValuePair<string, string>> descriptor);
    }
}