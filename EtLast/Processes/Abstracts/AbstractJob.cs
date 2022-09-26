namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractJob : AbstractProcess
{
    protected AbstractJob(IEtlContext context)
        : base(context)
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

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateParameters();

            if (!Pipe.IsTerminating)
            {
                ExecuteImpl(netTimeStopwatch);
            }
        }
        catch (Exception ex)
        {
            Pipe.AddException(this, ex);
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}/{ElapsedWallClock}",
            Kind, Pipe.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

    protected abstract void ExecuteImpl(Stopwatch netTimeStopwatch);
}