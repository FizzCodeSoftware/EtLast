namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public class MemorySortMutator : AbstractSequence, IMutator
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }
    public Func<IEnumerable<IRow>, IEnumerable<IRow>> Sorter { get; init; }

    public MemorySortMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (Sorter == null)
            throw new ProcessParameterNullException(this, nameof(Sorter));
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        var rows = new List<IRow>();

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

            if (row.Tag is HeartBeatTag)
            {
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
            rows.Add(row);
        }

        netTimeStopwatch.Start();

        Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount} of {TotalRowCount} rows in {Elapsed}/{ElapsedWallClock}",
            mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

        IEnumerator<IRow> sortedRowsEnumerator = null;
        try
        {
            sortedRowsEnumerator = Sorter(rows).GetEnumerator();
        }
        catch (Exception ex)
        {
            InvocationContext.AddException(this, new CustomCodeException(this, "error during the execution of custom sort code", ex));
        }

        if (sortedRowsEnumerator != null)
        {
            while (!InvocationContext.IsTerminating)
            {
                IRow row;
                try
                {
                    var finished = !sortedRowsEnumerator.MoveNext();
                    if (finished)
                        break;

                    row = sortedRowsEnumerator.Current;
                }
                catch (Exception ex)
                {
                    InvocationContext.AddException(this, new CustomCodeException(this, "error during the execution of custom sort code", ex));
                    break;
                }

                yield return row;
            }
        }

        Context.Log(LogSeverity.Debug, this, "sorted {RowCount} rows in {Elapsed}/{ElapsedWallClock}",
            mutatedRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
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

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class MemorySortMutatorFluent
{
    public static IFluentSequenceMutatorBuilder SortInMemory(this IFluentSequenceMutatorBuilder builder, MemorySortMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder SortInMemory(this IFluentSequenceMutatorBuilder builder, string name, Func<IEnumerable<IRow>, IEnumerable<IRow>> sorter)
    {
        return builder.AddMutator(new MemorySortMutator(builder.ProcessBuilder.Result.Context)
        {
            Name = name,
            Sorter = sorter,
        });
    }
}
