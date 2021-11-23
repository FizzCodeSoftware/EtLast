namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public abstract class AbstractEtlTask : AbstractProcess, IEtlTask
    {
        public IEtlSession Session { get; private set; }

        private readonly ExecutionStatistics _statistics = new();
        public IExecutionStatistics Statistics => _statistics;

        public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;
        public Dictionary<string, object> Output { get; } = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

        private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

        public abstract IEnumerable<IExecutable> CreateProcesses();

        protected AbstractEtlTask()
            : base()
        {
        }

        public TaskResult Execute(IProcess caller, IEtlSession session)
        {
            Session = session;
            Context = session.Context;

            Context.RegisterProcessInvocationStart(this, caller);

            var path = Name;
            var c = caller;
            while (c != null)
            {
                path = c.Name + "/" + path;
                c = c.InvocationInfo.Caller;
            }

            Context.Log(LogSeverity.Information, caller, "executing task {Task}", Name);

            Context.RegisterProcessInvocationStart(this, caller);
            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                _statistics.Start();

                Context.Listeners.Add(_ioCommandCounterCollection);
                var result = new TaskResult();
                try
                {
                    var executables = CreateProcesses()?.ToList();
                    if (executables?.Count > 0)
                    {
                        for (var executableIndex = 0; executableIndex < executables.Count; executableIndex++)
                        {
                            var executable = executables[executableIndex];
                            var originalExceptionCount = Context.ExceptionCount;

                            executable.Execute(this);

                            result.ExceptionCount = Context.ExceptionCount - originalExceptionCount;
                            if (result.ExceptionCount > 0)
                                break;
                        }
                    }
                }
                finally
                {
                    Session.Context.Listeners.Remove(_ioCommandCounterCollection);
                }

                _statistics.Finish();

                Context.Log(LogSeverity.Information, this, "task {TaskResult} in {Elapsed}",
                    (result.ExceptionCount == 0) ? "finished" : "failed", _statistics.RunTime);

                foreach (var kvp in Output)
                {
                    Context.Log(LogSeverity.Debug, this, "output [{key}] = [{value}]",
                        kvp.Key, kvp.Value ?? "NULL");
                }

                return result;
            }
            finally
            {
                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            }
        }
    }
}