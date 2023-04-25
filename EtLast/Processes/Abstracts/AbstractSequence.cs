namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSequence : AbstractProcess, ISequence
{
    public Action<ISequence> Initializer { get; init; }

    protected AbstractSequence(IEtlContext context)
        : base(context)
    {
    }

    private IEnumerable<IRow> Evaluate(IProcess caller, FlowState flowState)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        FlowState = flowState ?? caller?.FlowState ?? new FlowState(Context);

        LogCall(caller);
        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateImpl();
        }
        catch (Exception ex)
        {
            netTimeStopwatch.Stop();
            FlowState.AddException(this, ex);
        }

        if (!FlowState.IsTerminating)
        {
            if (Initializer != null)
            {
                try
                {
                    Initializer.Invoke(this);
                }
                catch (Exception ex)
                {
                    FlowState.AddException(this, new InitializerDelegateException(this, ex));
                }
            }

            if (!FlowState.IsTerminating)
            {
                IEnumerator<IRow> enumerator = null;
                try
                {
                    enumerator = EvaluateImpl(netTimeStopwatch).GetEnumerator();
                }
                catch (Exception ex)
                {
                    FlowState.AddException(this, ex);

                    netTimeStopwatch.Stop();
                    Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                    Context.Log(LogSeverity.Information, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                        FlowState.StatusToLogString(), InvocationInfo.InvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
                }

                if (enumerator != null)
                {
                    while (!FlowState.IsTerminating)
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
                            FlowState.AddException(this, ex);

                            netTimeStopwatch.Stop();
                            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                            Context.Log(LogSeverity.Information, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                                FlowState.StatusToLogString(), InvocationInfo.InvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
                        }

                        if (!FlowState.IsTerminating)
                        {
                            netTimeStopwatch.Stop();
                            var row = enumerator.Current;
                            yield return row;
                            netTimeStopwatch.Start();
                        }
                    }
                }
            }
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        Context.Log(LogSeverity.Information, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
            FlowState.StatusToLogString(), InvocationInfo.InvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

    protected abstract IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch);
    protected abstract void ValidateImpl();

    public override void Execute(IProcess caller)
    {
        CountRowsAndReleaseOwnership(caller);
    }

    public override void Execute(IProcess caller, FlowState flowState)
    {
        CountRowsAndReleaseOwnership(caller, flowState);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller)
    {
        return TakeRowsAndTransferOwnership(caller, caller?.FlowState);
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller)
    {
        return TakeRowsAndReleaseOwnership(caller, caller?.FlowState);
    }

    public int CountRowsAndReleaseOwnership(IProcess caller)
    {
        return CountRowsAndReleaseOwnership(caller, caller?.FlowState);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller, FlowState flowState)
    {
        foreach (var row in Evaluate(caller, flowState))
        {
            row.Context.SetRowOwner(row, caller);
            yield return row;
        }
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller, FlowState flowState)
    {
        foreach (var row in Evaluate(caller, flowState))
        {
            if (caller != null)
                row.Context.SetRowOwner(row, caller);

            row.Context.SetRowOwner(row, null);

            yield return row;
        }
    }

    public int CountRowsAndReleaseOwnership(IProcess caller, FlowState flowState)
    {
        var count = 0;
        foreach (var row in Evaluate(caller, flowState))
        {
            row.Context.SetRowOwner(row, caller);
            row.Context.SetRowOwner(row, null);

            count++;
        }

        return count;
    }
}