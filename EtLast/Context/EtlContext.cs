namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;

    public class EtlContext : IEtlContext
    {
        public Type RowType { get; private set; }
        public EtlContextResult Result { get; } = new EtlContextResult();
        public AdditionalData AdditionalData { get; }

        public DateTimeOffset CreatedOnUtc { get; }
        public DateTimeOffset CreatedOnLocal { get; }

        public List<IEtlContextListener> Listeners { get; } = new List<IEtlContextListener>();

        /// <summary>
        /// Default value: 10 minutes.
        /// </summary>
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(10);

        public CancellationTokenSource CancellationTokenSource { get; }

        private int _nextRowUid;
        private int _nextProcessInstanceUid;
        private int _nextProcessInvocationUid;
        private int _nextRowStoreUid;
        private int _nextIoCommandUid;
        private readonly List<Exception> _exceptions = new List<Exception>();
        private readonly Dictionary<string, int> _rowStores = new Dictionary<string, int>();

        public EtlContext()
        {
            SetRowType<DictionaryRow>();
            CancellationTokenSource = new CancellationTokenSource();
            AdditionalData = new AdditionalData();

            CreatedOnLocal = DateTimeOffset.Now;
            CreatedOnUtc = CreatedOnUtc.ToUniversalTime();
        }

        public void SetRowType<T>() where T : IRow
        {
            RowType = typeof(T);
        }

        /// <summary>
        /// Executes the specified process.
        /// </summary>
        /// <param name="terminateHostOnFail">If true, then failures will set the <see cref="EtlContextResult.TerminateHost"/> field to true in <see cref="Result"/>.</param>
        /// <param name="executable">The process to execute.</param>
        public void ExecuteOne(bool terminateHostOnFail, IExecutable executable)
        {
            var initialExceptionCount = ExceptionCount;

            try
            {
                Log(LogSeverity.Information, null, "executing {ProcessName}", executable.Name);
                executable.Execute(null);

                if (ExceptionCount > initialExceptionCount)
                {
                    Result.Success = false;
                    Result.TerminateHost = terminateHostOnFail;
                }
            }
            catch (Exception unhandledException)
            {
                AddException(executable, unhandledException);
                Result.Success = false;
                Result.TerminateHost = terminateHostOnFail;
            }
        }

        /// <summary>
        /// Sequentially executes the specified strategies in the specified order. In case of failure the execution will terminated.
        /// </summary>
        /// <param name="terminateHostOnFail">If true, then failures will set the <see cref="EtlContextResult.TerminateHost"/> field to true in <see cref="Result"/>.</param>
        /// <param name="executables">The processes to execute.</param>
        public void ExecuteSequence(bool terminateHostOnFail, params IExecutable[] executables)
        {
            var initialExceptionCount = ExceptionCount;

            foreach (var executable in executables)
            {
                Log(LogSeverity.Information, null, "executing {ProcessName}", executable.Name);

                try
                {
                    executable.Execute(null);

                    if (ExceptionCount > initialExceptionCount)
                        break;
                }
                catch (Exception unhandledException)
                {
                    AddException(executable, unhandledException);
                    Result.Success = false;
                    Result.TerminateHost = terminateHostOnFail;
                    break;
                }
            }

            if (ExceptionCount > initialExceptionCount)
            {
                Result.Success = false;
                Result.TerminateHost = terminateHostOnFail;
            }
        }

        public void Log(string transactionId, LogSeverity severity, IProcess process, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            foreach (var listener in Listeners)
            {
                listener.OnLog(severity, false, transactionId, process, text, args);
            }
        }

        public void Log(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            foreach (var listener in Listeners)
            {
                listener.OnLog(severity, false, null, process, text, args);
            }
        }

        public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            foreach (var listener in Listeners)
            {
                listener.OnLog(severity, true, null, process, text, args);
            }
        }

        public void LogCustom(string fileName, IProcess process, string text, params object[] args)
        {
            foreach (var listener in Listeners)
            {
                listener.OnCustomLog(false, fileName, process, text, args);
            }
        }

        public void LogCustomOps(string fileName, IProcess process, string text, params object[] args)
        {
            foreach (var listener in Listeners)
            {
                listener.OnCustomLog(true, fileName, process, text, args);
            }
        }

        public int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
        {
            var uid = Interlocked.Increment(ref _nextIoCommandUid);
            foreach (var listener in Listeners)
            {
                listener.OnContextIoCommandStart(uid, kind, location, null, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageArgs);
            }

            return uid;
        }

        public int RegisterIoCommandStart(IProcess process, IoCommandKind kind, string location, string path, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs)
        {
            var uid = Interlocked.Increment(ref _nextIoCommandUid);
            foreach (var listener in Listeners)
            {
                listener.OnContextIoCommandStart(uid, kind, location, path, process, timeoutSeconds, command, transactionId, argumentListGetter, message, messageArgs);
            }

            return uid;
        }

        public void RegisterIoCommandSuccess(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount)
        {
            foreach (var listener in Listeners)
            {
                listener.OnContextIoCommandEnd(process, uid, kind, affectedDataCount, null);
            }
        }

        public void RegisterIoCommandFailed(IProcess process, IoCommandKind kind, int uid, int? affectedDataCount, Exception exception)
        {
            foreach (var listener in Listeners)
            {
                listener.OnContextIoCommandEnd(process, uid, kind, affectedDataCount, exception);
            }
        }

        public void RegisterRowStored(IProcess process, IReadOnlyRow row, int storeUid)
        {
            foreach (var listener in Listeners)
            {
                listener.OnRowStored(process, row, storeUid);
            }
        }

        public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            var row = (IRow)Activator.CreateInstance(RowType);
            row.Init(this, process, Interlocked.Increment(ref _nextRowUid), initialValues);

            foreach (var listener in Listeners)
            {
                listener.OnRowCreated(row, process);
            }

            return row;
        }

        public IRow CreateRow(IProcess process, IReadOnlySlimRow source)
        {
            var row = (IRow)Activator.CreateInstance(RowType);
            row.Init(this, process, Interlocked.Increment(ref _nextRowUid), source.Values);
            row.Tag = source.Tag;

            foreach (var listener in Listeners)
            {
                listener.OnRowCreated(row, process);
            }

            return row;
        }

        public void AddException(IProcess process, Exception ex)
        {
            if (ex is OperationCanceledException)
                return;

            if (!(ex is EtlException))
            {
                ex = new ProcessExecutionException(process, ex);
            }

            Result.Exceptions.Add(ex);

            lock (_exceptions)
            {
                if (_exceptions.Contains(ex))
                {
                    CancellationTokenSource.Cancel();
                    return;
                }

                _exceptions.Add(ex);
            }

            foreach (var listener in Listeners)
            {
                listener.OnException(process, ex);
            }

            CancellationTokenSource.Cancel();
        }

        public List<Exception> GetExceptions()
        {
            lock (_exceptions)
            {
                return new List<Exception>(_exceptions);
            }
        }

        public int ExceptionCount
        {
            get
            {
                lock (_exceptions)
                {
                    return _exceptions.Count;
                }
            }
        }

        public EtlTransactionScope BeginScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity)
        {
            return new EtlTransactionScope(this, process, kind, TransactionScopeTimeout, logSeverity);
        }

        public void SetRowOwner(IRow row, IProcess currentProcess)
        {
            if (row.CurrentProcess == currentProcess)
                return;

            var previousProcess = row.CurrentProcess;
            row.CurrentProcess = currentProcess;

            foreach (var listener in Listeners)
            {
                listener.OnRowOwnerChanged(row, previousProcess, currentProcess);
            }
        }

        public void RegisterProcessInvocationStart(IProcess process, IProcess caller)
        {
            process.InvocationInfo = new ProcessInvocationInfo()
            {
                InstanceUid = process.InvocationInfo?.InstanceUid ?? Interlocked.Increment(ref _nextProcessInstanceUid),
                Number = (process.InvocationInfo?.Number ?? 0) + 1,
                InvocationUid = Interlocked.Increment(ref _nextProcessInvocationUid),
                LastInvocationStarted = Stopwatch.StartNew(),
                Caller = caller,
            };

            foreach (var listener in Listeners)
            {
                listener.OnProcessInvocationStart(process);
            }
        }

        public void RegisterProcessInvocationEnd(IProcess process)
        {
            process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;

            foreach (var listener in Listeners)
            {
                listener.OnProcessInvocationEnd(process);
            }
        }

        public void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds)
        {
            process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;
            process.InvocationInfo.LastInvocationNetTimeMilliseconds = netElapsedMilliseconds;

            foreach (var listener in Listeners)
            {
                listener.OnProcessInvocationEnd(process);
            }
        }

        public int GetStoreUid(string location, string path)
        {
            var key = location + " / " + path;
            if (!_rowStores.TryGetValue(key, out var storeUid))
            {
                storeUid = Interlocked.Increment(ref _nextRowStoreUid);
                _rowStores.Add(key, storeUid);
                foreach (var listener in Listeners)
                {
                    listener.OnRowStoreStarted(storeUid, location, path);
                }
            }

            return storeUid;
        }

        public void Close()
        {
            foreach (var listener in Listeners)
            {
                listener.OnContextClosed();
                if (listener is IDisposable disp)
                {
                    disp.Dispose();
                }
            }
        }
    }
}