namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMutator : AbstractProcess, IMutator
{
    public Action<ISequence> Initializer { get; init; }
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }

    protected AbstractMutator(IEtlContext context)
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
            if (Input == null)
                throw new ProcessParameterNullException(this, nameof(Input));

            ValidateMutator();
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
                try
                {
                    StartMutator();
                }
                catch (Exception ex)
                {
                    InvocationContext.AddException(this, ex);
                }
            }

            if (!InvocationContext.IsTerminating)
            {
                var mutatedRows = new List<IRow>();

                netTimeStopwatch.Stop();
                var enumerator = Input.TakeRowsAndTransferOwnership(this).GetEnumerator();
                netTimeStopwatch.Start();

                var mutatedRowCount = 0;
                var ignoredRowCount = 0;

                while (!InvocationContext.IsTerminating)
                {
                    netTimeStopwatch.Stop();
                    var finished = !enumerator.MoveNext();
                    if (finished)
                        break;

                    var row = enumerator.Current;
                    netTimeStopwatch.Start();

                    if (row.Tag is HeartBeatTag tag)
                    {
                        ProcessHeartBeatRow(row, tag);

                        netTimeStopwatch.Stop();
                        yield return row;
                        netTimeStopwatch.Start();
                        continue;
                    }

                    var apply = false;
                    if (RowFilter != null)
                    {
                        try
                        {
                            apply = RowFilter.Invoke(row);
                        }
                        catch (Exception ex)
                        {
                            InvocationContext.AddException(this, ex, row);
                            break;
                        }

                        if (!apply)
                        {
                            ignoredRowCount++;
                            netTimeStopwatch.Stop();
                            yield return row;
                            netTimeStopwatch.Start();
                            continue;
                        }
                    }

                    if (RowTagFilter != null)
                    {
                        try
                        {
                            apply = RowTagFilter.Invoke(row.Tag);
                        }
                        catch (Exception ex)
                        {
                            InvocationContext.AddException(this, ex, row);
                            break;
                        }

                        if (!apply)
                        {
                            ignoredRowCount++;
                            netTimeStopwatch.Stop();
                            yield return row;
                            netTimeStopwatch.Start();
                            continue;
                        }
                    }

                    mutatedRowCount++;

                    var kept = false;
                    try
                    {
                        foreach (var mutatedRow in MutateRow(row))
                        {
                            if (mutatedRow == row)
                                kept = true;

                            if (mutatedRow.CurrentProcess != this)
                            {
                                InvocationContext.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                                break;
                            }

                            mutatedRows.Add(mutatedRow);
                        }
                    }
                    catch (Exception ex)
                    {
                        InvocationContext.AddException(this, ex, row);
                        break;
                    }

                    if (!kept)
                    {
                        Context.SetRowOwner(row, null);
                    }

                    netTimeStopwatch.Stop();
                    foreach (var mutatedRow in mutatedRows)
                    {
                        yield return mutatedRow;
                    }

                    netTimeStopwatch.Start();

                    mutatedRows.Clear();
                }

                netTimeStopwatch.Start();

                try
                {
                    CloseMutator();
                }
                catch (Exception ex)
                {
                    InvocationContext.AddException(this, ex);
                }

                if (mutatedRowCount + ignoredRowCount > 0)
                {
                    Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount} of {TotalRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                        mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
                }
            }
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        Context.Log(LogSeverity.Information, this, "{ProcessKind} {ProcessResult} in {Elapsed}/{ElapsedWallClock}",
            Kind, InvocationContext.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

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

    protected virtual void ValidateMutator()
    {
    }

    protected virtual void StartMutator()
    {
    }

    protected virtual void CloseMutator()
    {
    }

    protected abstract IEnumerable<IRow> MutateRow(IRow row);

    protected virtual void ProcessHeartBeatRow(IReadOnlySlimRow row, HeartBeatTag tag)
    {
    }

    public IEnumerator<IMutator> GetEnumerator()
    {
        yield return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        yield return this;
    }
}
