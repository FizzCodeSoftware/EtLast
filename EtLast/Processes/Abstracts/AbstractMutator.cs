namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMutator : AbstractSequence, IMutator
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }

    protected AbstractMutator(IEtlContext context)
        : base(context)
    {
    }

    protected sealed override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            StartMutator();
        }
        catch (Exception ex)
        {
            AddException(ex);
            yield break;
        }

        var mutatedRows = new List<IRow>();

        netTimeStopwatch.Stop();
        var enumerator = Input.TakeRowsAndTransferOwnership(this).GetEnumerator();
        netTimeStopwatch.Start();

        var mutatedRowCount = 0;
        var ignoredRowCount = 0;

        while (!Context.CancellationToken.IsCancellationRequested)
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
                    AddException(ex, row);
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
                    AddException(ex, row);
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
                        AddException(new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                        break;
                    }

                    mutatedRows.Add(mutatedRow);
                }
            }
            catch (Exception ex)
            {
                AddException(ex, row);
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
            AddException(ex);
            yield break;
        }

        if (mutatedRowCount + ignoredRowCount > 0)
        {
            Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount} of {TotalRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
        }
    }

    protected sealed override void ValidateImpl()
    {
        if (Input == null)
            throw new ProcessParameterNullException(this, nameof(Input));

        ValidateMutator();
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
