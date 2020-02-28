namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;

    public class EtlContext : IEtlContext
    {
        public Type RowType { get; private set; }
        public StatCounterCollection CounterCollection { get; }
        public EtlContextResult Result { get; } = new EtlContextResult();
        public AdditionalData AdditionalData { get; }

        public DateTimeOffset CreatedOnUtc { get; }
        public DateTimeOffset CreatedOnLocal { get; }

        /// <summary>
        /// Default value: 10 minutes.
        /// </summary>
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(10);

        public CancellationTokenSource CancellationTokenSource { get; }

        public EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        public ContextOnLogDelegate OnLog { get; set; }
        public ContextOnCustomLogDelegate OnCustomLog { get; set; }

        public ContextOnRowCreatedDelegate OnRowCreated { get; set; }
        public ContextOnRowOwnerChangedDelegate OnRowOwnerChanged { get; set; }
        public ContextOnRowValueChangedDelegate OnRowValueChanged { get; set; }
        public ContextOnRowStoreStartedDelegate OnRowStoreStarted { get; set; }
        public ContextOnRowStoredDelegate OnRowStored { get; set; }
        public ContextOnProcessInvocationDelegate OnProcessInvocationStart { get; set; }
        public ContextOnProcessInvocationDelegate OnProcessInvocationEnd { get; set; }
        public ContextOnDataStoreCommandDelegate OnContextDataStoreCommand { get; set; }

        private int _nextRowUID;
        private int _nextProcessInstanceUID;
        private int _nextProcessInvocationUID;
        private int _nextRowStoreUID;
        private readonly List<Exception> _exceptions = new List<Exception>();
        private readonly Dictionary<string, int> _rowStores = new Dictionary<string, int>();

        public EtlContext(StatCounterCollection forwardCountersToCollection = null)
        {
            SetRowType<DictionaryRow>();
            CancellationTokenSource = new CancellationTokenSource();
            AdditionalData = new AdditionalData();

            var utcNow = DateTimeOffset.UtcNow;
            CreatedOnUtc = utcNow;
            CreatedOnLocal = utcNow.ToLocalTime();

            CounterCollection = new StatCounterCollection(forwardCountersToCollection);
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

        public void Log(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            OnLog?.Invoke(severity, false, false, process, text, args);
        }

        public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            OnLog?.Invoke(severity, true, false, process, text, args);
        }

        public void LogNoDiag(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            OnLog?.Invoke(severity, false, true, process, text, args);
        }

        public void LogCustom(string fileName, IProcess process, string text, params object[] args)
        {
            OnCustomLog?.Invoke(false, fileName, process, text, args);
        }

        public void LogCustomOps(string fileName, IProcess process, string text, params object[] args)
        {
            OnCustomLog?.Invoke(true, fileName, process, text, args);
        }

        public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            var row = (IRow)Activator.CreateInstance(RowType);
            row.Init(this, process, Interlocked.Increment(ref _nextRowUID), initialValues);

            CounterCollection.IncrementCounter("in-memory rows created", 1);

            OnRowCreated?.Invoke(row, process);

            return row;
        }

        public void AddException(IProcess process, Exception ex)
        {
            if (ex is OperationCanceledException)
                return;

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

            OnException?.Invoke(this, new ContextExceptionEventArgs()
            {
                Process = process,
                Exception = ex,
            });

            CounterCollection.IncrementCounter("exceptions", 1);

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
            OnRowOwnerChanged?.Invoke(row, previousProcess, currentProcess);
        }

        public void RegisterProcessInvocationStart(IProcess process, IProcess caller)
        {
            process.InvocationInfo = new ProcessInvocationInfo()
            {
                InstanceUID = process.InvocationInfo?.InstanceUID ?? Interlocked.Increment(ref _nextProcessInstanceUID),
                Number = (process.InvocationInfo?.Number ?? 0) + 1,
                InvocationUID = Interlocked.Increment(ref _nextProcessInvocationUID),
                LastInvocationStarted = Stopwatch.StartNew(),
                Caller = caller,
            };

            OnProcessInvocationStart?.Invoke(process);
        }

        public void RegisterProcessInvocationEnd(IProcess process)
        {
            process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;
            OnProcessInvocationEnd?.Invoke(process);
        }

        public void RegisterProcessInvocationEnd(IProcess process, long netElapsedMilliseconds)
        {
            process.InvocationInfo.LastInvocationFinished = DateTimeOffset.Now;
            process.InvocationInfo.LastInvocationNetTimeMilliseconds = netElapsedMilliseconds;
            OnProcessInvocationEnd?.Invoke(process);
        }

        public int GetStoreUid(List<KeyValuePair<string, string>> descriptor)
        {
            var key = string.Join(",", descriptor.Select(x => x.Key + "=" + x.Value));
            if (!_rowStores.TryGetValue(key, out var storeUid))
            {
                storeUid = Interlocked.Increment(ref _nextRowStoreUID);
                _rowStores.Add(key, storeUid);
                OnRowStoreStarted?.Invoke(storeUid, descriptor);
            }

            return storeUid;
        }
    }
}