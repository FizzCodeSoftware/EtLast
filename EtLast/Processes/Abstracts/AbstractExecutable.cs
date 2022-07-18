namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractExecutable : AbstractProcess, IExecutable
{
    protected AbstractExecutable(IEtlContext context)
        : base(context)
    {
    }

    public void Execute(IProcess caller = null)
    {
        Context.RegisterProcessInvocationStart(this, caller);

        if (caller != null)
            Context.Log(LogSeverity.Information, this, "process started by {Process}", caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "process started");

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateImpl();

            if (Context.CancellationToken.IsCancellationRequested)
                return;

            ExecuteImpl();
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
    }

    protected abstract void ExecuteImpl();
    protected abstract void ValidateImpl();
}
