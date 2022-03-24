namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public abstract class AbstractEtlTask : AbstractProcess, IEtlTask
    {
        public IEtlSession Session { get; private set; }

        private readonly ExecutionStatistics _statistics = new();
        public IExecutionStatistics Statistics => _statistics;

        public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

        private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

        public abstract IEnumerable<IExecutable> CreateProcesses();

        protected AbstractEtlTask()
        {
        }

        public abstract void ValidateParameters();

        public ProcessResult Execute(IProcess caller, IEtlSession session)
        {
            Session = session;
            Context = session.Context;

            Context.RegisterProcessInvocationStart(this, caller);

            if (caller != null)
                Context.Log(LogSeverity.Information, this, "task started by {Process}", caller.Name);
            else
                Context.Log(LogSeverity.Information, this, "task started");

            LogPublicSettableProperties(LogSeverity.Debug);

            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                _statistics.Start();

                ValidateParameters();

                Context.Listeners.Add(_ioCommandCounterCollection);
                var exceptionCount = 0;
                try
                {
                    var executables = CreateProcesses()?
                        .Where(x => x != null)
                        .ToList();

                    if (executables?.Count > 0)
                    {
                        for (var executableIndex = 0; executableIndex < executables.Count; executableIndex++)
                        {
                            var executable = executables[executableIndex];
                            var originalExceptionCount = Context.ExceptionCount;

                            executable.Execute(this);

                            exceptionCount = Context.ExceptionCount - originalExceptionCount;
                            if (exceptionCount > 0)
                                break;
                        }
                    }
                }
                finally
                {
                    Session.Context.Listeners.Remove(_ioCommandCounterCollection);
                }

                var result = new ProcessResult()
                {
                    ExceptionCount = exceptionCount,
                };

                _statistics.Finish();

                Context.Log(LogSeverity.Information, this, "task {TaskResult} in {Elapsed}",
                    (result.ExceptionCount == 0) ? "finished" : "failed", _statistics.RunTime);

                LogPrivateSettableProperties(LogSeverity.Debug);

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