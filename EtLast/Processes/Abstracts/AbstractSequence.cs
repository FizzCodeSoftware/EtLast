namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSequence : AbstractProcess, ISequence
{
    public Action<ISequence> Initializer { get; init; }

    protected AbstractSequence(IEtlContext context)
        : base(context)
    {
    }

    private IEnumerable<IRow> Evaluate(IProcess caller)
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
        }
        catch (Exception ex)
        {
            netTimeStopwatch.Stop();
            AddException(ex);
            yield break;
        }

        if (Context.CancellationToken.IsCancellationRequested)
            yield break;

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
                yield break;
        }

        IEnumerator<IRow> enumerator;
        try
        {
            enumerator = EvaluateImpl(netTimeStopwatch).GetEnumerator();
        }
        catch (Exception ex)
        {
            AddException(ex);

            netTimeStopwatch.Stop();
            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}",
                Kind, "failed", InvocationInfo.LastInvocationStarted.Elapsed);

            yield break;
        }

        while (!Context.CancellationToken.IsCancellationRequested)
        {
            try
            {
                netTimeStopwatch.Stop();
                var finished = !enumerator.MoveNext();
                netTimeStopwatch.Start();
                if (finished)
                    break;
            }
            catch (Exception ex)
            {
                AddException(ex);

                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}",
                    Kind, "failed", InvocationInfo.LastInvocationStarted.Elapsed);

                yield break;
            }

            var row = enumerator.Current;
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
        CountRowsAndReleaseOwnership(caller);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller)
    {
        foreach (var row in Evaluate(caller))
        {
            row.Context.SetRowOwner(row, caller);
            yield return row;
        }
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller)
    {
        foreach (var row in Evaluate(caller))
        {
            if (caller != null)
                row.Context.SetRowOwner(row, caller);

            row.Context.SetRowOwner(row, null);

            yield return row;
        }
    }

    public int CountRowsAndReleaseOwnership(IProcess caller)
    {
        var count = 0;
        foreach (var row in Evaluate(caller))
        {
            row.Context.SetRowOwner(row, caller);
            row.Context.SetRowOwner(row, null);

            count++;
        }

        return count;
    }
}