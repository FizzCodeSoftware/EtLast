namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractProcessWithResult<T> : AbstractProcess, IProcessWithResult<T>
{
    protected AbstractProcessWithResult(IEtlContext context)
        : base(context)
    {
    }

    public override void Execute(IProcess caller)
    {
        ExecuteWithResult(caller, null);
    }

    public override void Execute(IProcess caller, FlowState flowState)
    {
        ExecuteWithResult(caller, flowState);
    }

    public T ExecuteWithResult(IProcess caller)
    {
        return ExecuteWithResult(caller, null);
    }

    public T ExecuteWithResult(IProcess caller, FlowState flowState)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        FlowState = flowState ?? caller?.FlowState ?? new FlowState(Context);

        LogCall(caller);
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