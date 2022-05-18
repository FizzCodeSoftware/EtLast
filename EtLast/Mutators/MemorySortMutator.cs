namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public class MemorySortMutator : AbstractEvaluable, IMutator
{
    public IProducer InputProcess { get; set; }
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
        var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
        netTimeStopwatch.Start();

        var mutatedRowCount = 0;
        var ignoredRowCount = 0;

        while (!Context.CancellationTokenSource.IsCancellationRequested)
        {
            netTimeStopwatch.Stop();
            var finished = !enumerator.MoveNext();
            netTimeStopwatch.Start();
            if (finished)
                break;

            var row = enumerator.Current;

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
                    Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
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
                    Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
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

        Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount} of {TotalRowCount} rows in {Elapsed}/{ElapsedWallClock}",
            mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

        IEnumerator<IRow> sortedRowsEnumerator = null;
        try
        {
            sortedRowsEnumerator = Sorter(rows).GetEnumerator();
        }
        catch (Exception ex)
        {
            Context.AddException(this, new CustomCodeException(this, "error during the execution of custom sort code", ex));
        }

        if (sortedRowsEnumerator != null)
        {
            while (!Context.CancellationTokenSource.IsCancellationRequested)
            {
                IRow row;
                try
                {
                    netTimeStopwatch.Stop();
                    var finished = !sortedRowsEnumerator.MoveNext();
                    netTimeStopwatch.Start();
                    if (finished)
                        break;

                    row = sortedRowsEnumerator.Current;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new CustomCodeException(this, "error during the execution of custom sort code", ex));
                    break;
                }

                yield return row;
            }
        }

        netTimeStopwatch.Stop();
        Context.Log(LogSeverity.Debug, this, "sorted {RowCount} rows in {Elapsed}/{ElapsedWallClock}",
            mutatedRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
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
    public static IFluentProcessMutatorBuilder SortInMemory(this IFluentProcessMutatorBuilder builder, MemorySortMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentProcessMutatorBuilder SortInMemory(this IFluentProcessMutatorBuilder builder, string name, Func<IEnumerable<IRow>, IEnumerable<IRow>> sorter)
    {
        return builder.AddMutator(new MemorySortMutator(builder.ProcessBuilder.Result.Context)
        {
            Name = name,
            Sorter = sorter,
        });
    }
}
