namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMutator : AbstractProcess, IMutator
{
    public Action<ISequence> Initializer { get; init; }

    [ProcessParameterNullException]
    public ISequence Input { get; set; }

    public RowTestDelegate RowFilter { get; set; }

    public RowTagTestDelegate RowTagFilter { get; set; }

    protected AbstractMutator(IEtlContext context)
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
            ValidateParameterAnnotations();
            ValidateParameters();
        }
        catch (Exception ex)
        {
            netTimeStopwatch.Stop();
            FlowState.AddException(this, ex);
        }

        if (FlowState.IsTerminating)
        {
            LogResult(netTimeStopwatch);
            yield break;
        }

        if (Initializer != null)
        {
            try
            {
                Initializer.Invoke(this);
            }
            catch (Exception ex)
            {
                netTimeStopwatch.Stop();
                FlowState.AddException(this, new InitializerDelegateException(this, ex));
            }
        }

        if (FlowState.IsTerminating)
        {
            LogResult(netTimeStopwatch);
            yield break;
        }

        try
        {
            StartMutator();
        }
        catch (Exception ex)
        {
            FlowState.AddException(this, ex);
        }

        if (FlowState.IsTerminating)
        {
            LogResult(netTimeStopwatch);
            yield break;
        }

        netTimeStopwatch.Stop();
        var enumerator = Input.TakeRowsAndTransferOwnership(this).GetEnumerator();
        netTimeStopwatch.Start();

        var ignoredRowCount = 0;
        var removedRowCount = 0;
        var keptRowCount = 0;
        var addedRowCount = 0;
        var mutatedRows = new List<IRow>();

        while (!FlowState.IsTerminating)
        {
            netTimeStopwatch.Stop();
            var finished = !enumerator.MoveNext();
            if (finished)
                break;

            var row = enumerator.Current;
            netTimeStopwatch.Start();

            if (row.Tag is HeartBeatTag tag)
            {
                ProcessHeartBeatTag(tag);

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
                    FlowState.AddException(this, ex, row);
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
                    FlowState.AddException(this, ex, row);
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

            var kept = false;
            try
            {
                foreach (var mutatedRow in MutateRow(row))
                {
                    if (mutatedRow == row)
                    {
                        keptRowCount++;
                        kept = true;
                    }
                    else
                    {
                        addedRowCount++;
                    }

                    if (mutatedRow.CurrentProcess != this)
                    {
                        FlowState.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                        break;
                    }

                    mutatedRows.Add(mutatedRow);
                }
            }
            catch (Exception ex)
            {
                FlowState.AddException(this, ex, row);
                break;
            }

            if (!kept)
            {
                removedRowCount++;
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
            FlowState.AddException(this, ex);
        }

        if (ignoredRowCount + keptRowCount + removedRowCount > 0)
        {
            Context.Log(LogSeverity.Debug, this, "processed {MutatedRowCount} of {TotalRowCount} input rows: {IgnoredRowCount} ignored, {KeptRowCount} kept, {RemovedRowCount} removed, {AddedRowCount} added",
                keptRowCount + removedRowCount, ignoredRowCount + keptRowCount + removedRowCount, ignoredRowCount, keptRowCount, removedRowCount, addedRowCount);
        }

        LogResult(netTimeStopwatch);
    }

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

    protected virtual void StartMutator()
    {
    }

    protected virtual void CloseMutator()
    {
    }

    protected abstract IEnumerable<IRow> MutateRow(IRow row);

    protected virtual void ProcessHeartBeatTag(HeartBeatTag tag)
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