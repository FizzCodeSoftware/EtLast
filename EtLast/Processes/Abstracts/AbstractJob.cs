﻿namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractJob : AbstractProcess, IJob
{
    protected AbstractJob(IEtlContext context)
        : base(context)
    {
    }

    public void Execute(IProcess caller = null)
    {
        Context.RegisterProcessInvocationStart(this, caller);

        if (caller != null)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process}", Kind, caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        var originalExceptionCount = Context.ExceptionCount;
        try
        {
            ValidateImpl();

            if (Context.IsTerminating)
                return;

            ExecuteImpl(netTimeStopwatch);
        }
        catch (Exception ex)
        {
            AddException(ex);
        }
        finally
        {
            netTimeStopwatch.Stop();
            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }

        Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}/{ElapsedWallClock}",
            Kind, Context.ExceptionCount == originalExceptionCount ? "finished" : "failed", InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

    protected abstract void ExecuteImpl(Stopwatch netTimeStopwatch);
    protected abstract void ValidateImpl();
}
