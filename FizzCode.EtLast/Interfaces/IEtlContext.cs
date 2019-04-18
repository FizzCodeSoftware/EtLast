namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Threading;

    public interface IEtlContext
    {
        Configuration Configuration { get; }
        CancellationTokenSource CancellationTokenSource { get; }
        ConnectionStringSettings GetConnectionStringSettings(string key);

        bool GetParameter(string name, out object value);
        void SetParameter(string name, object value);

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