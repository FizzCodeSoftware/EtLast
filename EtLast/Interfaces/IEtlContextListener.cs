namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Extensions.Configuration;

    public interface IEtlContextListener
    {
        bool Init(IExecutionContext executionContext, IConfigurationSection configurationSection);

        void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args);
        void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args);
        void OnException(IProcess process, Exception exception);

        void OnRowCreated(IReadOnlyRow row, IProcess process);
        void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess);
        void OnRowValueChanged(IProcess process, IReadOnlyRow row, params KeyValuePair<string, object>[] values);
        void OnRowStoreStarted(int storeUid, string location, string path);
        void OnRowStored(IProcess process, IReadOnlyRow row, int storeUid);

        void OnProcessInvocationStart(IProcess process);
        void OnProcessInvocationEnd(IProcess process);
        void OnContextIoCommandStart(int uid, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs);
        void OnContextIoCommandEnd(IProcess process, int uid, IoCommandKind kind, int? affectedDataCount, Exception ex);

        void OnContextClosed(); // all pending state must be flushed in this method
    }
}