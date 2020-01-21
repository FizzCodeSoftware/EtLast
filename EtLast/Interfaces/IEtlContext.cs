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
        EtlTransactionScope BeginScope(IProcess process, IBaseOperation operation, TransactionScopeKind kind, LogSeverity logSeverity);

        CancellationTokenSource CancellationTokenSource { get; }

        void ExecuteOne(bool terminateHostOnFail, IExecutable executable);
        void ExecuteSequence(bool terminateHostOnFail, params IExecutable[] executables);

        IRow CreateRow(IProcess creatorProcess, IEnumerable<KeyValuePair<string, object>> initialValues);

        void Log(LogSeverity severity, IProcess process, string text, params object[] args);
        void Log(LogSeverity severity, IProcess process, IBaseOperation operation, string text, params object[] args);
        void LogOps(LogSeverity severity, IProcess process, string text, params object[] args);
        void LogOps(LogSeverity severity, IProcess process, IBaseOperation operation, string text, params object[] args);

        void LogCustom(string fileName, IProcess process, string text, params object[] args);
        void LogCustomOps(string fileName, IProcess process, string text, params object[] args);

        void AddException(IProcess process, Exception ex, IBaseOperation operation = null);
        List<Exception> GetExceptions();

        int ExceptionCount { get; }

        void SetRowOwner(IRow row, IProcess currentProcess);

        EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        ContextOnLogDelegate OnLog { get; set; }
        ContextOnCustomLogDelegate OnCustomLog { get; set; }

        ContextOnRowCreatedDelegate OnRowCreated { get; set; }
        ContextOnRowOwnerChangedDelegate OnRowOwnerChanged { get; set; }
        ContextOnRowValueChangedDelegate OnRowValueChanged { get; set; }
        ContextOnRowStoredDelegate OnRowStored { get; set; }

        int GetProcessUid(IProcess process);
    }
}