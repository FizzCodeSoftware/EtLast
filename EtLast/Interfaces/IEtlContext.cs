namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Threading;

    public interface IEtlContext
    {
        Configuration Configuration { get; }
        StatCounterCollection Stat { get; }
        EtlContextResult Result { get; }

        DateTimeOffset CreatedOnUtc { get; }
        DateTimeOffset CreatedOnLocal { get; }

        TimeSpan TransactionScopeTimeout { get; }
        CancellationTokenSource CancellationTokenSource { get; }
        ConnectionStringSettings GetConnectionStringSettings(string key);

        void ExecuteOne(bool terminateHostOnFail, IEtlStrategy strategy);
        void ExecuteSequence(bool terminateHostOnFail, params IEtlStrategy[] strategies);

        IRow CreateRow(int columnCountHint = 0);
        void Log(LogSeverity severity, IProcess process, string text, params object[] args);
        void LogOps(LogSeverity severity, IProcess process, string text, params object[] args);
        void LogRow(IProcess process, IRow row, string text, params object[] args);

        void AddException(IProcess process, Exception ex);
        List<Exception> GetExceptions();

        EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        EventHandler<ContextLogEventArgs> OnLog { get; set; }
    }
}