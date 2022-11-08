namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSequence : AbstractProcess, ISequence
{
    public Action<ISequence> Initializer { get; init; }

    protected AbstractSequence(IEtlContext context)
        : base(context)
    {
    }

    private IEnumerable<IRow> Evaluate(IProcess caller, Pipe pipe)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        Pipe = pipe ?? caller?.Pipe ?? new Pipe(Context);

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
            Pipe.AddException(this, ex);
        }

        if (!Pipe.IsTerminating)
        {
            if (Initializer != null)
            {
                try
                {
                    Initializer.Invoke(this);
                }
                catch (Exception ex)
                {
                    Pipe.AddException(this, new InitializerDelegateException(this, ex));
                }
            }

            if (!Pipe.IsTerminating)
            {
                IEnumerator<IRow> enumerator = null;
                try
                {
                    enumerator = EvaluateImpl(netTimeStopwatch).GetEnumerator();
                }
                catch (Exception ex)
                {
                    Pipe.AddException(this, ex);

                    netTimeStopwatch.Stop();
                    Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                    Context.Log(LogSeverity.Information, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                        Pipe.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
                }

                if (enumerator != null)
                {
                    while (!Pipe.IsTerminating)
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
                            Pipe.AddException(this, ex);

                            netTimeStopwatch.Stop();
                            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                            Context.Log(LogSeverity.Information, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                                Pipe.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
                        }

                        if (!Pipe.IsTerminating)
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
            Pipe.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

    protected abstract IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch);
    protected abstract void ValidateImpl();

    public override void Execute(IProcess caller)
    {
        CountRowsAndReleaseOwnership(caller);
    }

    public override void Execute(IProcess caller, Pipe pipe)
    {
        CountRowsAndReleaseOwnership(caller, pipe);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller)
    {
        return TakeRowsAndTransferOwnership(caller, caller?.Pipe);
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller)
    {
        return TakeRowsAndReleaseOwnership(caller, caller?.Pipe);
    }

    public int CountRowsAndReleaseOwnership(IProcess caller)
    {
        return CountRowsAndReleaseOwnership(caller, caller?.Pipe);
    }

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller, Pipe pipe)
    {
        foreach (var row in Evaluate(caller, pipe))
        {
            row.Context.SetRowOwner(row, caller);
            yield return row;
        }
    }

    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller, Pipe pipe)
    {
        foreach (var row in Evaluate(caller, pipe))
        {
            if (caller != null)
                row.Context.SetRowOwner(row, caller);

            row.Context.SetRowOwner(row, null);

            yield return row;
        }
    }

    public int CountRowsAndReleaseOwnership(IProcess caller, Pipe pipe)
    {
        var count = 0;
        foreach (var row in Evaluate(caller, pipe))
        {
            row.Context.SetRowOwner(row, caller);
            row.Context.SetRowOwner(row, null);

            count++;
        }

        return count;
    }
}