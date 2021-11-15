﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class IoCommandCounterCollection : IEtlContextListener
    {
        public Dictionary<IoCommandKind, IoCommandCounter> Counters { get; } = new Dictionary<IoCommandKind, IoCommandCounter>();

        public void OnContextIoCommandStart(int uid, IoCommandKind kind, string location, string path, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
        {
        }

        public void OnContextIoCommandEnd(IProcess process, int uid, IoCommandKind kind, int? affectedDataCount, Exception ex)
        {
            Counters.TryGetValue(kind, out var counter);
            if (counter == null)
            {
                Counters[kind] = counter = new IoCommandCounter();
            }

            counter.InvocationCount++;

            if (affectedDataCount != null)
            {
                var cnt = (counter.AffectedDataCount ?? 0) + affectedDataCount.Value;
                counter.AffectedDataCount = cnt;
            }
        }

        public void OnContextClosed()
        {
        }

        public void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args)
        {
        }

        public void OnException(IProcess process, Exception exception)
        {
        }

        public void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args)
        {
        }

        public void OnProcessInvocationEnd(IProcess process)
        {
        }

        public void OnProcessInvocationStart(IProcess process)
        {
        }

        public void OnRowCreated(IReadOnlyRow row)
        {
        }

        public void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess)
        {
        }

        public void OnWriteToSink(IReadOnlyRow row, int sinkUid)
        {
        }

        public void OnSinkStarted(int sinkUid, string location, string path)
        {
        }

        public void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values)
        {
        }
    }
}