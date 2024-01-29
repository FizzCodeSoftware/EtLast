namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSequence : AbstractProcess, ISequence
{
    public Action<ISequence> Initializer { get; init; }

    protected AbstractSequence()
    {
    }

    private IEnumerable<IRow> Evaluate(ICaller caller, FlowState flowState = null)
    {
        BeginExecution(caller, flowState);
        if (FlowState.IsTerminating)
            yield break;

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            ValidateParameterAnnotations();
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

                    LogResult(netTimeStopwatch);
                    yield break;
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

                            LogResult(netTimeStopwatch);
                            yield break;
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

        LogResult(netTimeStopwatch);
    }

    protected abstract IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch);
    protected abstract void ValidateImpl();

    public override void Execute(ICaller caller, FlowState flowState = null)
    {
        CountRowsAndReleaseOwnership(caller, flowState);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(ICaller caller, FlowState flowState = null)
    {
        foreach (var row in Evaluate(caller, flowState))
        {
            row.SetOwner(caller as IProcess);
            yield return row;
        }
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(ICaller caller, FlowState flowState = null)
    {
        foreach (var row in Evaluate(caller, flowState))
        {
            if (caller is IProcess callerProcess)
                row.SetOwner(callerProcess);

            row.SetOwner(null);

            yield return row;
        }
    }

    public int CountRowsAndReleaseOwnership(ICaller caller, FlowState flowState = null)
    {
        var count = 0;
        foreach (var row in Evaluate(caller, flowState))
        {
            row.SetOwner(caller as IProcess);
            row.SetOwner(null);

            count++;
        }

        return count;
    }
}