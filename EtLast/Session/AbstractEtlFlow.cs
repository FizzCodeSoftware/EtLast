namespace FizzCode.EtLast;

public abstract class AbstractEtlFlow : AbstractProcess, IEtlFlow
{
    public IEtlSession Session { get; private set; }

    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

    private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

    public abstract void Execute();

    protected AbstractEtlFlow()
    {
    }

    public abstract void ValidateParameters();

    public ProcessResult Execute(IProcess caller, IEtlSession session)
    {
        Session = session;
        Context = session.Context;

        Context.RegisterProcessInvocationStart(this, caller);

        if (caller != null)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process}", Kind, caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);

        LogPublicSettableProperties(LogSeverity.Debug);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            _statistics.Start();

            ValidateParameters();

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

            var result = new ProcessResult();
            result.Exceptions.AddRange(Context.GetExceptions().Skip(originalExceptionCount));

            _statistics.Finish();
            Context.Log(LogSeverity.Information, this, "{ProcessKind} {TaskResult} in {Elapsed}",
                Kind, (Exceptions.Count == 0) ? "finished" : "failed", _statistics.RunTime);

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
