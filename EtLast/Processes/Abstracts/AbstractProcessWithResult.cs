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

    public override void Execute(IProcess caller, Pipe pipe)
    {
        ExecuteWithResult(caller, pipe);
    }

    public T ExecuteWithResult(IProcess caller)
    {
        return ExecuteWithResult(caller, null);
    }

    public T ExecuteWithResult(IProcess caller, Pipe pipe)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        Pipe = pipe ?? caller?.Pipe ?? new Pipe(Context);

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        T result = default;
        try
        {
            ValidateImpl();

            if (!Pipe.IsTerminating)
            {
                result = ExecuteImpl();
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

        return result;
    }

    protected abstract void ValidateImpl();
    protected abstract T ExecuteImpl();
}