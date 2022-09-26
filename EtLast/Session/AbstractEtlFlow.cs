namespace FizzCode.EtLast;

public abstract class AbstractEtlFlow : AbstractProcess, IEtlFlow
{
    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

    private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

    public abstract void Execute();

    protected AbstractEtlFlow()
    {
    }

    public override void Execute(IProcess caller, Pipe pipe)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        Pipe = pipe ?? caller?.Pipe ?? new Pipe(Context);

        if (caller != null)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process}", Kind, caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);

        LogPublicSettableProperties(LogSeverity.Debug);

        var netTimeStopwatch = Stopwatch.StartNew();
        _statistics.Start();

        try
        {
            ValidateParameters();
        }
        catch (Exception ex)
        {
            Pipe.AddException(this, ex);
        }

        if (!Pipe.IsTerminating)
        {
            Context.Listeners.Add(_ioCommandCounterCollection);
            try
            {
                Execute();
            }
            catch (Exception ex)
            {
                Pipe.AddException(this, ex);
            }
            finally
            {
                Context.Listeners.Remove(_ioCommandCounterCollection);
            }

            _statistics.Finish();
            Context.Log(LogSeverity.Information, this, "{ProcessKind} {TaskResult} in {Elapsed}",
                Kind, Pipe.ToLogString(), _statistics.RunTime);

            LogPrivateSettableProperties(LogSeverity.Debug);
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
    }

    public IPipeStarter NewPipe()
    {
        var pipe = new Pipe(Context);
        return new PipeBuilder(this, pipe);
    }
}