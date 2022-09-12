namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSequence : AbstractProcess, ISequence
{
    public Action<ISequence> Initializer { get; init; }

    protected AbstractSequence(IEtlContext context)
        : base(context)
    {
    }

    private IEnumerable<IRow> Evaluate(IProcess caller, ProcessInvocationContext invocationContext)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        InvocationContext = invocationContext ?? caller?.InvocationContext ?? new ProcessInvocationContext(Context);

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
            InvocationContext.AddException(this, ex);
        }

        if (!InvocationContext.IsTerminating)
        {
            if (Initializer != null)
            {
                try
                {
                    Initializer.Invoke(this);
                }
                catch (Exception ex)
                {
                    InvocationContext.AddException(this, new InitializerDelegateException(this, ex));
                }
            }

            if (!InvocationContext.IsTerminating)
            {
                IEnumerator<IRow> enumerator = null;
                try
                {
                    enumerator = EvaluateImpl(netTimeStopwatch).GetEnumerator();
                }
                catch (Exception ex)
                {
                    InvocationContext.AddException(this, ex);

                    netTimeStopwatch.Stop();
                    Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                    Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                        Kind, "failed", InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
                }

                while (enumerator != null && !InvocationContext.IsTerminating)
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
                        InvocationContext.AddException(this, ex);

                        netTimeStopwatch.Stop();
                        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                        Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                            Kind, "failed", InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
                    }

                    if (!InvocationContext.IsTerminating)
                    {
                        netTimeStopwatch.Stop();
                        var row = enumerator.Current;
                        yield return row;
                        netTimeStopwatch.Start();
                    }
                }
            }
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}/{ElapsedWallClock}",
            Kind, InvocationContext.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

    protected abstract IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch);
    protected abstract void ValidateImpl();

    public void Execute(IProcess caller)
    {
        CountRowsAndReleaseOwnership(caller);
    }

    public void Execute(IProcess caller, ProcessInvocationContext invocationContext)
    {
        CountRowsAndReleaseOwnership(caller, invocationContext);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller)
    {
        return TakeRowsAndTransferOwnership(caller, caller?.InvocationContext);
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller)
    {
        return TakeRowsAndReleaseOwnership(caller, caller?.InvocationContext);
    }

    public int CountRowsAndReleaseOwnership(IProcess caller)
    {
        return CountRowsAndReleaseOwnership(caller, caller?.InvocationContext);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller, ProcessInvocationContext invocationContext)
    {
        foreach (var row in Evaluate(caller, invocationContext))
        {
            row.Context.SetRowOwner(row, caller);
            yield return row;
        }
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller, ProcessInvocationContext invocationContext)
    {
        foreach (var row in Evaluate(caller, invocationContext))
        {
            if (caller != null)
                row.Context.SetRowOwner(row, caller);

            row.Context.SetRowOwner(row, null);

            yield return row;
        }
    }

    public int CountRowsAndReleaseOwnership(IProcess caller, ProcessInvocationContext invocationContext)
    {
        var count = 0;
        foreach (var row in Evaluate(caller, invocationContext))
        {
            row.Context.SetRowOwner(row, caller);
            row.Context.SetRowOwner(row, null);

            count++;
        }

        return count;
    }
}