namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public abstract class AbstractEtlFlow : AbstractProcess, IEtlFlow
    {
        public IEtlSession Session { get; private set; }

        private readonly ExecutionStatistics _statistics = new();
        public IExecutionStatistics Statistics => _statistics;

        public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;
        public Dictionary<string, object> Output { get; } = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

        private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

        public abstract void Execute();

        protected AbstractEtlFlow()
        {
        }

        public TaskResult Execute(IProcess caller, IEtlSession session)
        {
            Session = session;
            Context = session.Context;

            var path = Name;
            var c = caller;
            while (c != null)
            {
                path = c.Name + "/" + path;
                c = c.InvocationInfo.Caller;
            }

            Context.Log(LogSeverity.Information, caller, "executing flow {Task}", Name);

            Context.RegisterProcessInvocationStart(this, caller);
            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                _statistics.Start();

                Context.Listeners.Add(_ioCommandCounterCollection);
                var originalExceptionCount = Context.ExceptionCount;
                try
                {
                    Execute();
                }
                finally
                {
                    Session.Context.Listeners.Remove(_ioCommandCounterCollection);
                }

                var taskResult = new TaskResult()
                {
                    ExceptionCount = Context.ExceptionCount - originalExceptionCount,
                };

                _statistics.Finish();
                Context.Log(LogSeverity.Information, this, "flow {TaskResult} in {Elapsed}",
                    (taskResult.ExceptionCount == 0) ? "finished" : "failed", _statistics.RunTime);

                foreach (var kvp in Output)
                {
                    Context.Log(LogSeverity.Debug, this, "output [{key}] = [{value}]",
                        kvp.Key, kvp.Value ?? "NULL");
                }

                return taskResult;
            }
            finally
            {
                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            }
        }
    }
}