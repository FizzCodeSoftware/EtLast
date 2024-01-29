namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMutator : AbstractProcess, IMutator
{
    public Action<ISequence> Initializer { get; init; }

    [ProcessParameterMustHaveValue]
    public ISequence Input { get; set; }

    public RowTestDelegate RowFilter { get; set; }

    public RowTagTestDelegate RowTagFilter { get; set; }

    protected AbstractMutator()
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

        var rowFilter = RowFilter;

        var rowInputIndex = -1;

        while (!FlowState.IsTerminating)
        {
            netTimeStopwatch.Stop();
            var finished = !enumerator.MoveNext();
            if (finished)
                break;

            rowInputIndex++;

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
            if (rowFilter != null)
            {
                try
                {
                    apply = rowFilter.Invoke(row);
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
                foreach (var mutatedRow in MutateRow(row, rowInputIndex))
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

                    if (mutatedRow.Owner != this)
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
                row.SetOwner(null);
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

    protected virtual void StartMutator()
    {
    }

    protected virtual void CloseMutator()
    {
    }

    protected abstract IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex);

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