namespace FizzCode.EtLast;

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

            var result = new ProcessResult();

            Context.Listeners.Add(_ioCommandCounterCollection);
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

                        var newExceptions = Context.GetExceptions().Skip(originalExceptionCount).ToList();
                        if (newExceptions.Count > 0)
                        {
                            result.Exceptions.AddRange(newExceptions);
                            break;
                        }
                    }
                }
            }
            finally
            {
                Session.Context.Listeners.Remove(_ioCommandCounterCollection);
            }

            _statistics.Finish();

            Context.Log(LogSeverity.Information, this, "task {TaskResult} in {Elapsed}",
                (result.Exceptions.Count == 0) ? "finished" : "failed", _statistics.RunTime);

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
