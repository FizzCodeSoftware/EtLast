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

    private IEnumerable<IRow> Evaluate(IProcess caller, Pipe pipe)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        Pipe = pipe ?? caller?.Pipe ?? new Pipe(Context);

        if (caller is IEtlTask)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Task}", Kind, caller.Name);
        else if (caller != null)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process}", Kind, caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);

        LogPublicSettableProperties(LogSeverity.Verbose);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            if (Input == null)
                throw new ProcessParameterNullException(this, nameof(Input));

            ValidateParameters();
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
                try
                {
                    StartMutator();
                }
                catch (Exception ex)
                {
                    Pipe.AddException(this, ex);
                }
            }

            if (!Pipe.IsTerminating)
            {
                var mutatedRows = new List<IRow>();

                netTimeStopwatch.Stop();
                var enumerator = Input.TakeRowsAndTransferOwnership(this).GetEnumerator();
                netTimeStopwatch.Start();

                var mutatedRowCount = 0;
                var ignoredRowCount = 0;

                while (!Pipe.IsTerminating)
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
                            Pipe.AddException(this, ex, row);
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
                            Pipe.AddException(this, ex, row);
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
                                Pipe.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                                break;
                            }

                            mutatedRows.Add(mutatedRow);
                        }
                    }
                    catch (Exception ex)
                    {
                        Pipe.AddException(this, ex, row);
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
                    Pipe.AddException(this, ex);
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
            Kind, Pipe.ToLogString(), InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
    }

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
