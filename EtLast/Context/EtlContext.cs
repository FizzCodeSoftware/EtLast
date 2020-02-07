namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
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
        public ContextOnDataStoreCommandDelegate OnContextDataStoreCommand { get; set; }

        public ContextOnRowCreatedDelegate OnRowCreated { get; set; }
        public ContextOnRowOwnerChangedDelegate OnRowOwnerChanged { get; set; }
        public ContextOnRowValueChangedDelegate OnRowValueChanged { get; set; }
        public ContextOnRowStoredDelegate OnRowStored { get; set; }
        public ContextOnProcessCreatedDelegate OnProcessCreated { get; set; }
        public ContextOnOperationCreatedDelegate OnOperationCreated { get; set; }

        private int _nextRowUid;
        private int _nextProcessUid;
        private int _nextOperationUid;
        private readonly List<Exception> _exceptions = new List<Exception>();

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

            OnLog?.Invoke(severity, false, process, null, text, args);
        }

        public void Log(LogSeverity severity, IProcess process, IOperation operation, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            OnLog?.Invoke(severity, false, process, operation, text, args);
        }

        public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            OnLog?.Invoke(severity, true, process, null, text, args);
        }

        public void LogOps(LogSeverity severity, IProcess process, IOperation operation, string text, params object[] args)
        {
            OnLog?.Invoke(severity, true, process, operation, text, args);
        }

        public void LogCustom(string fileName, IProcess process, string text, params object[] args)
        {
            OnCustomLog?.Invoke(false, fileName, process, text, args);
        }

        public void LogCustomOps(string fileName, IProcess process, string text, params object[] args)
        {
            OnCustomLog?.Invoke(true, fileName, process, text, args);
        }

        public void LogDataStoreCommand(string location, IProcess process, IOperation operation, string command, IEnumerable<KeyValuePair<string, object>> args)
        {
            OnContextDataStoreCommand?.Invoke(location, process, operation, command, args);
        }

        public IRow CreateRow(IOperation operation, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            return CreateRowInternal(operation.Process, operation, initialValues);
        }

        public IRow CreateRow(IProcess process, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            return CreateRowInternal(process, null, initialValues);
        }

        private IRow CreateRowInternal(IProcess process, IOperation operation, IEnumerable<KeyValuePair<string, object>> initialValues)
        {
            var row = (IRow)Activator.CreateInstance(RowType);
            row.Init(this, process, Interlocked.Increment(ref _nextRowUid) - 1, initialValues);

            CounterCollection.IncrementCounter("in-memory rows created", 1);

            OnRowCreated?.Invoke(row, process, operation);

            return row;
        }

        public void AddException(IProcess process, Exception ex, IOperation operation = null)
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
                Operation = operation,
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

        public EtlTransactionScope BeginScope(IProcess process, IOperation operation, TransactionScopeKind kind, LogSeverity logSeverity)
        {
            return new EtlTransactionScope(this, process, operation, kind, TransactionScopeTimeout, logSeverity);
        }

        public void SetRowOwner(IRow row, IProcess currentProcess)
        {
            var previousProcess = row.CurrentProcess;
            row.CurrentProcess = currentProcess;
            OnRowOwnerChanged?.Invoke(row, previousProcess, currentProcess, null);
        }

        public void SetRowOwner(IRow row, IProcess currentProcess, IOperation operation)
        {
            var previousProcess = row.CurrentProcess;
            row.CurrentProcess = currentProcess;
            OnRowOwnerChanged?.Invoke(row, previousProcess, currentProcess, operation);
        }

        public int GetProcessUid(IProcess process)
        {
            var uid = Interlocked.Increment(ref _nextProcessUid) - 1;
            OnProcessCreated?.Invoke(uid, process);
            return uid;
        }

        public int GetOperationUid(IOperation operation)
        {
            var uid = Interlocked.Increment(ref _nextOperationUid) - 1;
            OnOperationCreated?.Invoke(uid, operation);
            return uid;
        }
    }
}