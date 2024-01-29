namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractJob : AbstractProcess
{
    protected AbstractJob()
    {
    }

    public override void Execute(ICaller caller, FlowState flowState = null)
    {
        BeginExecution(caller, flowState);
        if (FlowState.IsTerminating)
            return;

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();

        try
        {
            ValidateParameterAnnotations();
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

        LogResult(netTimeStopwatch);
    }

    protected abstract void ExecuteImpl(Stopwatch netTimeStopwatch);
}