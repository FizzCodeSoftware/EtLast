namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
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
        if (FlowState.IsTerminating)
            return default;

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        T result = default;
        try
        {
            ValidateParameterAnnotations();
            ValidateParameters();

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

    protected abstract T ExecuteImpl();
}