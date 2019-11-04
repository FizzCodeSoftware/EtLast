namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using FizzCode.DbTools.Configuration;

    public class EtlContext<TRow> : IEtlContext
        where TRow : IRow, new()
    {
        private readonly List<Exception> _exceptions = new List<Exception>();
        public StatCounterCollection Stat { get; } = new StatCounterCollection();
        public EtlContextResult Result { get; } = new EtlContextResult();
        public AdditionalData AdditionalData { get; }

        public ConnectionStringCollection ConnectionStrings { get; set; }

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

        public EtlContext()
        {
            CancellationTokenSource = new CancellationTokenSource();
            AdditionalData = new AdditionalData();

            var utcNow = DateTimeOffset.UtcNow;
            CreatedOnUtc = utcNow;
            CreatedOnLocal = utcNow.ToLocalTime();
        }

        /// <summary>
        /// Executes the specified strategy.
        /// </summary>
        /// <param name="terminateHostOnFail">If true, then a failed strategy will set the <see cref="EtlContextResult.TerminateHost"/> field to true in the result object.</param>
        /// <param name="strategy">The strategy to be executed.</param>
        public void ExecuteOne(bool terminateHostOnFail, IEtlStrategy strategy)
        {
            var initialExceptionCount = GetExceptions().Count;

            try
            {
                strategy.Execute(null);

                if (GetExceptions().Count > initialExceptionCount)
                {
                    Result.Success = false;
                    Result.TerminateHost = terminateHostOnFail;
                }
            }
            catch (Exception unhandledException)
            {
                Result.Success = false;
                Result.TerminateHost = terminateHostOnFail;
                Result.Exceptions.Add(unhandledException);
            }
        }

        /// <summary>
        /// Sequentially executes the specified strategies in the specified order.
        /// If a strategy fails then the execution will stop and return to the caller.
        /// </summary>
        /// <param name="terminateHostOnFail">If true, then a failed strategy will set the <see cref="EtlContextResult.TerminateHost"/> field to true in the result object.</param>
        /// <param name="strategies">The strategies to be executed.</param>
        public void ExecuteSequence(bool terminateHostOnFail, params IEtlStrategy[] strategies)
        {
            var initialExceptionCount = GetExceptions().Count;

            try
            {
                foreach (var strategy in strategies)
                {
                    strategy.Execute(null);

                    var exceptions = GetExceptions();
                    if (exceptions.Count > initialExceptionCount)
                        break;
                }

                if (GetExceptions().Count > initialExceptionCount)
                {
                    Result.Success = false;
                    Result.TerminateHost = terminateHostOnFail;
                }
            }
            catch (Exception unhandledException)
            {
                Result.Success = false;
                Result.TerminateHost = terminateHostOnFail;
                Result.Exceptions.Add(unhandledException);
            }
        }

        public void Log(LogSeverity severity, IExecutionBlock caller, string text, params object[] args)
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

        public void Log(LogSeverity severity, IExecutionBlock caller, IJob job, IBaseOperation operation, string text, params object[] args)
        {
            if (severity == LogSeverity.Error || severity == LogSeverity.Warning)
                Result.WarningCount++;

            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Caller = caller,
                Job = job,
                Operation = operation,
                Text = text,
                Severity = severity,
                Arguments = args,
            });
        }

        public void LogOps(LogSeverity severity, IExecutionBlock caller, string text, params object[] args)
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

        public void LogOps(LogSeverity severity, IExecutionBlock caller, IJob job, IBaseOperation operation, string text, params object[] args)
        {
            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Caller = caller,
                Job = job,
                Operation = operation,
                Text = text,
                Severity = severity,
                Arguments = args,
                ForOps = true,
            });
        }

        public void LogRow(IProcess process, IRow row, string text, params object[] args)
        {
            var rowTemplate = "UID={UID}, " + (row.Flagged ? "FLAGGED, " : string.Empty) + string.Join(", ", row.Values.Select(kvp => kvp.Key + "={" + kvp.Key + "Value} ({" + kvp.Key + "Type}) "));
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

        public void LogCustom(string fileName, IExecutionBlock caller, string text, params object[] args)
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

        public void LogCustomOps(string fileName, IExecutionBlock caller, string text, params object[] args)
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

            Stat.IncrementCounter("in-memory rows created", 1);

            return row;
        }

        public void AddException(IProcess process, Exception ex)
        {
            if (ex is OperationCanceledException)
                return;

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

            Stat.IncrementCounter("exceptions", 1);

            CancellationTokenSource.Cancel();
        }

        public List<Exception> GetExceptions()
        {
            lock (_exceptions)
            {
                return new List<Exception>(_exceptions);
            }
        }

        public ConnectionStringWithProvider GetConnectionString(string key)
        {
            return ConnectionStrings?[key + "-" + Environment.MachineName] ?? ConnectionStrings?[key];
        }

        public EtlTransactionScope BeginScope(IExecutionBlock caller, IJob job, IBaseOperation operation, TransactionScopeKind kind, LogSeverity logSeverity)
        {
            return new EtlTransactionScope(this, caller, job, operation, kind, TransactionScopeTimeout, logSeverity);
        }
    }
}