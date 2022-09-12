namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractJob : AbstractProcess, IJob
{
    protected AbstractJob(IEtlContext context)
        : base(context)
    {
    }

    public void Execute(IProcess caller)
    {
        Execute(caller, null);
    }

    public void Execute(IProcess caller, ProcessInvocationContext invocationContext)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        InvocationContext = invocationContext ?? caller?.InvocationContext ?? new ProcessInvocationContext(Context);

        if (caller != null)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process}", Kind, caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateImpl();

            if (!InvocationContext.IsTerminating)
            {
                ExecuteImpl(netTimeStopwatch);
            }
        }
        catch (Exception ex)
        {
            InvocationContext.AddException(this, ex);
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}/{ElapsedWallClock}",
            Kind, InvocationContext.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

    protected abstract void ExecuteImpl(Stopwatch netTimeStopwatch);
    protected abstract void ValidateImpl();
}