namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSequence : AbstractProcess, ISequence
{
    public virtual bool ConsumerShouldNotBuffer { get; }
    public Action<ISequence> Initializer { get; init; }

    protected AbstractSequence(IEtlContext context)
        : base(context)
    {
    }

    public Evaluator Evaluate(IProcess caller = null)
    {
        Context.RegisterProcessInvocationStart(this, caller);

        if (caller != null)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process}", Kind, caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateImpl();

            if (Context.CancellationToken.IsCancellationRequested)
                return new Evaluator();

            if (Initializer != null)
            {
                try
                {
                    Initializer.Invoke(this);
                }
                catch (Exception ex)
                {
                    throw new InitializerDelegateException(this, ex);
                }

                if (Context.CancellationToken.IsCancellationRequested)
                    return new Evaluator();
            }

            return new Evaluator(caller, EvaluateInternal(netTimeStopwatch));
        }
        catch (Exception ex)
        {
            AddException(ex);

            netTimeStopwatch.Stop();
            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}",
                Kind, "failed", InvocationInfo.LastInvocationStarted.Elapsed);

            return new Evaluator();
        }
    }

    private IEnumerable<IRow> EvaluateInternal(Stopwatch netTimeStopwatch)
    {
        var enumerable = EvaluateImpl(netTimeStopwatch);
        foreach (var row in enumerable)
        {
            yield return row;
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}",
            Kind, "finished", InvocationInfo.LastInvocationStarted.Elapsed);
    }

    protected abstract IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch);
    protected abstract void ValidateImpl();

    public void Execute(IProcess caller)
    {
        var evaluator = Evaluate(caller);
        evaluator.ExecuteWithoutTransfer();
    }
}
