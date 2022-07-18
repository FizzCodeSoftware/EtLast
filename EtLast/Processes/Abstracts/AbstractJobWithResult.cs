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
        ExecuteWithResult(caller);
    }

    public T ExecuteWithResult(IProcess caller = null)
    {
        Context.RegisterProcessInvocationStart(this, caller);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateImpl();

            if (Context.CancellationToken.IsCancellationRequested)
                return default;

            return ExecuteImpl();
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

        return default;
    }

    protected abstract void ValidateImpl();
    protected abstract T ExecuteImpl();
}
