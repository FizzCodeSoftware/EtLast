﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public interface IEtlContext
    {
        string UID { get; }

        StatCounterCollection CounterCollection { get; }
        EtlContextResult Result { get; }
        AdditionalData AdditionalData { get; }

        DateTimeOffset CreatedOnUtc { get; }
        DateTimeOffset CreatedOnLocal { get; }

        TimeSpan TransactionScopeTimeout { get; }
        EtlTransactionScope BeginScope(IProcess caller, IBaseOperation operation, TransactionScopeKind kind, LogSeverity logSeverity);

        CancellationTokenSource CancellationTokenSource { get; }

        void ExecuteOne(bool terminateHostOnFail, IExecutable executable);
        void ExecuteSequence(bool terminateHostOnFail, params IExecutable[] executables);

        IRow CreateRow(int columnCountHint = 0);

        void Log(LogSeverity severity, IProcess caller, string text, params object[] args);
        void Log(LogSeverity severity, IProcess caller, IBaseOperation operation, string text, params object[] args);
        void LogOps(LogSeverity severity, IProcess caller, string text, params object[] args);
        void LogOps(LogSeverity severity, IProcess caller, IBaseOperation operation, string text, params object[] args);

        void LogRow(IProcess process, IRow row, string text, params object[] args);
        void LogCustom(string fileName, IProcess caller, string text, params object[] args);
        void LogCustomOps(string fileName, IProcess caller, string text, params object[] args);

        void AddException(IProcess process, Exception ex);
        List<Exception> GetExceptions();

        int ExceptionCount { get; }

        EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        EventHandler<ContextLogEventArgs> OnLog { get; set; }
    }
}