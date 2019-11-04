namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using FizzCode.DbTools.Configuration;

    public interface IEtlContext
    {
        StatCounterCollection Stat { get; }
        EtlContextResult Result { get; }
        AdditionalData AdditionalData { get; }

        DateTimeOffset CreatedOnUtc { get; }
        DateTimeOffset CreatedOnLocal { get; }

        TimeSpan TransactionScopeTimeout { get; }
        EtlTransactionScope BeginScope(IExecutionBlock caller, IJob job, IBaseOperation operation, TransactionScopeKind kind, LogSeverity logSeverity);

        CancellationTokenSource CancellationTokenSource { get; }
        ConnectionStringWithProvider GetConnectionString(string key);

        void ExecuteOne(bool terminateHostOnFail, IEtlStrategy strategy);
        void ExecuteSequence(bool terminateHostOnFail, params IEtlStrategy[] strategies);

        IRow CreateRow(int columnCountHint = 0);

        void Log(LogSeverity severity, IExecutionBlock caller, string text, params object[] args);
        void Log(LogSeverity severity, IExecutionBlock caller, IJob job, IBaseOperation operation, string text, params object[] args);
        void LogOps(LogSeverity severity, IExecutionBlock caller, string text, params object[] args);
        void LogOps(LogSeverity severity, IExecutionBlock caller, IJob job, IBaseOperation operation, string text, params object[] args);

        void LogRow(IProcess process, IRow row, string text, params object[] args);
        void LogCustom(string fileName, IExecutionBlock caller, string text, params object[] args);
        void LogCustomOps(string fileName, IExecutionBlock caller, string text, params object[] args);

        void AddException(IProcess process, Exception ex);
        List<Exception> GetExceptions();

        EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        EventHandler<ContextLogEventArgs> OnLog { get; set; }
    }
}