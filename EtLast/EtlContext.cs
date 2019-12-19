namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;

    public class EtlContext<TRow> : IEtlContext
        where TRow : IRow, new()
    {
        public string UID { get; } = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

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
        public EventHandler<ContextLogEventArgs> OnLog { get; set; }
        public EventHandler<ContextCustomLogEventArgs> OnCustomLog { get; set; }

        private int _nextUid;
        private readonly List<Exception> _exceptions = new List<Exception>();

        public EtlContext(StatCounterCollection forwardCountersToCollection = null)
        {
            CancellationTokenSource = new CancellationTokenSource();
            AdditionalData = new AdditionalData();

            var utcNow = DateTimeOffset.UtcNow;
            CreatedOnUtc = utcNow;
            CreatedOnLocal = utcNow.ToLocalTime();

            CounterCollection = new StatCounterCollection(forwardCountersToCollection);
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

        public void Log(LogSeverity severity, IProcess caller, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Caller = caller,
                Text = text,
                Severity = severity,
                Arguments = args,
            });
        }

        public void Log(LogSeverity severity, IProcess caller, IBaseOperation operation, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Caller = caller,
                Operation = operation,
                Text = text,
                Severity = severity,
                Arguments = args,
            });
        }

        public void LogOps(LogSeverity severity, IProcess caller, string text, params object[] args)
        {
            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Caller = caller,
                Text = text,
                Severity = severity,
                Arguments = args,
                ForOps = true,
            });
        }

        public void LogOps(LogSeverity severity, IProcess caller, IBaseOperation operation, string text, params object[] args)
        {
            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Caller = caller,
                Operation = operation,
                Text = text,
                Severity = severity,
                Arguments = args,
                ForOps = true,
            });
        }

        public void LogRow(IProcess process, IRow row, string text, params object[] args)
        {
            var rowTemplate = "UID={UID}, " + (row.Flagged ? "FLAGGED, " : "") + string.Join(", ", row.Values.Select(kvp => kvp.Key + "={" + kvp.Key + "Value} ({" + kvp.Key + "Type}) "));
            var rowArgs = new List<object> { row.UID };
            foreach (var kvp in row.Values)
            {
                if (kvp.Value != null)
                {
                    rowArgs.Add(kvp.Value);
                    rowArgs.Add(TypeHelpers.GetFriendlyTypeName(kvp.Value.GetType()));
                }
                else
                {
                    rowArgs.Add("NULL");
                    rowArgs.Add("-");
                }
            }

            Log(LogSeverity.Warning, null, text + " // " + rowTemplate, args.Concat(rowArgs).ToArray());
        }

        public void LogCustom(string fileName, IProcess caller, string text, params object[] args)
        {
            OnCustomLog?.Invoke(this, new ContextCustomLogEventArgs()
            {
                FileName = fileName,
                Caller = caller,
                Text = text,
                Arguments = args,
                ForOps = false,
            });
        }

        public void LogCustomOps(string fileName, IProcess caller, string text, params object[] args)
        {
            OnCustomLog?.Invoke(this, new ContextCustomLogEventArgs()
            {
                FileName = fileName,
                Caller = caller,
                Text = text,
                Arguments = args,
                ForOps = true,
            });
        }

        public IRow CreateRow(int columnCountHint)
        {
            var row = new TRow();
            row.Init(this, Interlocked.Increment(ref _nextUid) - 1, columnCountHint);

            CounterCollection.IncrementCounter("in-memory rows created", 1);

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

        public EtlTransactionScope BeginScope(IProcess caller, IBaseOperation operation, TransactionScopeKind kind, LogSeverity logSeverity)
        {
            return new EtlTransactionScope(this, caller, operation, kind, TransactionScopeTimeout, logSeverity);
        }
    }
}