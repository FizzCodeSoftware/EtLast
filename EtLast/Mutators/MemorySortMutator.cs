namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public class MemorySortMutator : AbstractSequence, IMutator
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }

    public required Func<IEnumerable<IRow>, IEnumerable<IRow>> Sorter { get; init; }

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

        while (!FlowState.IsTerminating)
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

            mutatedRowCount++;
            rows.Add(row);
        }

        netTimeStopwatch.Start();

        Context.Log(LogSeverity.Debug, this, "collected {MutatedRowCount} of {TotalRowCount} rows",
            mutatedRowCount, mutatedRowCount + ignoredRowCount);

        IEnumerator<IRow> sortedRowsEnumerator = null;
        try
        {
            sortedRowsEnumerator = Sorter(rows).GetEnumerator();
        }
        catch (Exception ex)
        {
            FlowState.AddException(this, new CustomCodeException(this, "error during the execution of custom sort code", ex));
        }

        if (sortedRowsEnumerator != null)
        {
            while (!FlowState.IsTerminating)
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
                    FlowState.AddException(this, new CustomCodeException(this, "error during the execution of custom sort code", ex));
                    break;
                }

                yield return row;
            }
        }

        Context.Log(LogSeverity.Debug, this, "sorted {RowCount} rows",
            mutatedRowCount);
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

[EditorBrowsable(EditorBrowsableState.Never)]
public static class MemorySortMutatorFluent
{
    public static IFluentSequenceMutatorBuilder SortInMemory(this IFluentSequenceMutatorBuilder builder, MemorySortMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder SortInMemory(this IFluentSequenceMutatorBuilder builder, string name, Func<IEnumerable<IRow>, IEnumerable<IRow>> sorter)
    {
        return builder.AddMutator(new MemorySortMutator()
        {
            Name = name,
            Sorter = sorter,
        });
    }
}
