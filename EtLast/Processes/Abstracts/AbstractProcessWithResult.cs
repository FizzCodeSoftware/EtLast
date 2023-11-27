namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractProcessWithResult<T> : AbstractProcess, IProcessWithResult<T>
{
    protected AbstractProcessWithResult()
    {
    }

    public override void Execute(ICaller caller, FlowState flowState = null)
    {
        ExecuteWithResult(caller, flowState);
    }

    public T ExecuteWithResult(ICaller caller, FlowState flowState = null)
    {
        BeginExecution(caller, flowState);
        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        T result = default;
        try
        {
            ValidateImpl();

            if (!FlowState.IsTerminating)
            {
                result = ExecuteImpl();
            }
        }
        catch (Exception ex)
        {
            FlowState.AddException(this, ex);
        }

        LogResult(netTimeStopwatch);
        return result;
    }

    protected abstract void ValidateImpl();
    protected abstract T ExecuteImpl();
}