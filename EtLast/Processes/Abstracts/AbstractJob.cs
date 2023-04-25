namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractJob : AbstractProcess
{
    protected AbstractJob(IEtlContext context)
        : base(context)
    {
    }

    public override void Execute(IProcess caller, FlowState flowState)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        FlowState = flowState ?? caller?.FlowState ?? new FlowState(Context);

        LogCall(caller);
        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateParameters();

            if (!FlowState.IsTerminating)
            {
                ExecuteImpl(netTimeStopwatch);
            }
        }
        catch (Exception ex)
        {
            FlowState.AddException(this, ex);
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        Context.Log(LogSeverity.Information, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
            FlowState.StatusToLogString(), InvocationInfo.InvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

    protected abstract void ExecuteImpl(Stopwatch netTimeStopwatch);
}