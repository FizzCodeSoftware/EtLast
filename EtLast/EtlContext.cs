namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class EtlContext<TRow> : IEtlContext
        where TRow : IRow, new()
    {
        private ConcurrentBag<Exception> Exceptions { get; } = new ConcurrentBag<Exception>();
        public StatCounterCollection Stat { get; } = new StatCounterCollection();
        public EtlContextResult Result { get; } = new EtlContextResult();

        public Configuration Configuration { get; }

        public DateTimeOffset CreatedOnUtc { get; }
        public DateTimeOffset CreatedOnLocal { get; }

        /// <summary>
        /// Default value: 10 minutes.
        /// </summary>
        public TimeSpan TransactionScopeTimeout { get; set; } = TimeSpan.FromMinutes(10);

        public CancellationTokenSource CancellationTokenSource { get; }

        public EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        public EventHandler<ContextLogEventArgs> OnLog { get; set; }

        private int _nextUid;

        public EtlContext()
            : this(ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None))
        {
        }

        public EtlContext(Configuration configuration)
        {
            CancellationTokenSource = new CancellationTokenSource();
            Configuration = configuration;

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
                strategy.Execute(this);

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
                    strategy.Execute(this);

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

        public void Log(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Process = process,
                Text = text,
                Severity = severity,
                Arguments = args,
            });
        }

        public void LogOps(LogSeverity severity, IProcess process, string text, params object[] args)
        {
            OnLog?.Invoke(this, new ContextLogEventArgs()
            {
                Process = process,
                Text = text,
                Severity = severity,
                Arguments = args,
                ForOps = true,
            });
        }

        public void LogRow(IProcess process, IRow row, string text, params object[] args)
        {
            var rowTemplate = "row {UID} " + (row.Flagged ? "(flagged) " : string.Empty) + string.Join(", ", row.Values.Select(kvp => "[" + kvp.Key + "] = ({" + kvp.Key + "Type}) {" + kvp.Key + "Value}"));
            var rowArgs = new List<object> { row.UID };
            foreach (var kvp in row.Values)
            {
                if (kvp.Value != null)
                {
                    rowArgs.Add(kvp.Value.GetType().Name);
                    rowArgs.Add(kvp.Value);
                }
                else
                {
                    rowArgs.Add("-");
                    rowArgs.Add("NULL");
                }
            }

            Log(LogSeverity.Warning, null, text + " // " + rowTemplate, args.Concat(rowArgs).ToArray());
        }

        public IRow CreateRow(int columnCountHint)
        {
            var row = new TRow();
            row.Init(this, Interlocked.Increment(ref _nextUid) - 1, columnCountHint);

            Stat.IncrementCounter("rows created", 1);

            return row;
        }

        public void AddException(IProcess process, Exception ex)
        {
            Exceptions.Add(ex);
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
            return new List<Exception>(Exceptions);
        }

        public ConnectionStringSettings GetConnectionStringSettings(string key)
        {
            return Configuration.ConnectionStrings.ConnectionStrings[key + "-" + Environment.MachineName] ?? Configuration.ConnectionStrings.ConnectionStrings[key];
        }

        public TransactionScope BeginScope(TransactionScopeKind kind)
        {
            return kind == TransactionScopeKind.None
                ? null
                : new TransactionScope((TransactionScopeOption)kind, TransactionScopeTimeout);
        }
    }
}