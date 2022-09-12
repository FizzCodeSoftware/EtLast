namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractJobWithResult<T> : AbstractProcess, IJobWithResult<T>
{
    protected AbstractJobWithResult(IEtlContext context)
        : base(context)
    {
    }

    public void Execute(IProcess caller)
    {
        ExecuteWithResult(caller, null);
    }

    public void Execute(IProcess caller, ProcessInvocationContext invocationContext)
    {
        ExecuteWithResult(caller, invocationContext);
    }

    public T ExecuteWithResult(IProcess caller)
    {
        return ExecuteWithResult(caller, null);
    }

    public T ExecuteWithResult(IProcess caller, ProcessInvocationContext invocationContext)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        InvocationContext = invocationContext ?? caller?.InvocationContext ?? new ProcessInvocationContext(Context);

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        T result = default;
        try
        {
            ValidateImpl();

            if (!InvocationContext.IsTerminating)
            {
                result = ExecuteImpl();
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

        return result;
    }

    protected abstract void ValidateImpl();
    protected abstract T ExecuteImpl();
}