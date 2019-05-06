namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Threading;

    public class EtlContext<TRow> : IEtlContext
        where TRow : IRow, new()
    {
        private readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
        private ConcurrentBag<Exception> Exceptions { get; } = new ConcurrentBag<Exception>();

        public Configuration Configuration { get; }

        public DateTimeOffset CreatedOnUtc { get; }
        public DateTimeOffset CreatedOnLocal { get; }

        public CancellationTokenSource CancellationTokenSource { get; }

        public EventHandler<ContextExceptionEventArgs> OnException { get; set; }
        public EventHandler<ContextLogEventArgs> OnLog { get; set; }

        private int _nextUid;

        public EtlContext()
            : this(ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None))
        {
            var utcNow = DateTimeOffset.UtcNow;
            CreatedOnUtc = utcNow;
            CreatedOnLocal = utcNow.ToLocalTime();
        }

        public EtlContext(Configuration configuration)
        {
            CancellationTokenSource = new CancellationTokenSource();

            Configuration = configuration;
        }

        public bool GetParameter(string name, out object value)
        {
            if (_parameters.TryGetValue(name, out value)) return true;
            return false;
        }

        public void SetParameter(string name, object value)
        {
            _parameters[name] = value;
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
    }
}