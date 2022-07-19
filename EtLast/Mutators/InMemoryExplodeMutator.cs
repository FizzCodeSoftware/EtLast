namespace FizzCode.EtLast;

public delegate IEnumerable<ISlimRow> InMemoryExplodeDelegate(InMemoryExplodeMutator process, IReadOnlyList<IReadOnlySlimRow> rows);

/// <summary>
/// Useful only for small amount of data due to all input rows are collected into a List and processed at once.
/// </summary>
public sealed class InMemoryExplodeMutator : AbstractSequence, IMutator
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }

    public InMemoryExplodeDelegate Action { get; init; }

    /// <summary>
    /// Default true.
    /// </summary>
    public bool RemoveOriginalRow { get; init; } = true;

    public InMemoryExplodeMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (Input == null)
            throw new ProcessParameterNullException(this, nameof(Input));

        if (Action == null)
            throw new ProcessParameterNullException(this, nameof(Action));
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        netTimeStopwatch.Stop();
        var sourceEnumerator = Input.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
        netTimeStopwatch.Start();

        var ignoredRowCount = 0;
        var rows = new List<IReadOnlySlimRow>();
        while (!Context.CancellationToken.IsCancellationRequested)
        {
            netTimeStopwatch.Stop();
            var finished = !sourceEnumerator.MoveNext();
            netTimeStopwatch.Start();
            if (finished)
                break;

            var row = sourceEnumerator.Current;

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

            rows.Add(row);
        }

        var resultCount = 0;

        netTimeStopwatch.Stop();
        var enumerator = Action.Invoke(this, rows).GetEnumerator();
        netTimeStopwatch.Start();

        if (!RemoveOriginalRow)
        {
            netTimeStopwatch.Stop();
            foreach (var row in rows)
            {
                yield return row as IRow;
            }

            netTimeStopwatch.Start();
            resultCount += rows.Count;
        }

        while (!Context.CancellationToken.IsCancellationRequested)
        {
            ISlimRow newRow;
            try
            {
                if (!enumerator.MoveNext())
                    break;

                newRow = enumerator.Current;
            }
            catch (Exception ex)
            {
                AddException(ex);
                break;
            }

            resultCount++;
            netTimeStopwatch.Stop();
            yield return Context.CreateRow(this, newRow);
            netTimeStopwatch.Start();
        }

        Context.Log(LogSeverity.Debug, this, "processed {InputRowCount} rows and returned {RowCount} rows in {Elapsed}/{ElapsedWallClock}",
            rows.Count, resultCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
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
public static class InMemoryExplodeMutatorFluent
{
    /// <summary>
    /// Create any number of new rows based on the input rows.
    /// <para>- memory footprint is high because all rows are collected before the delegate is called</para>
    /// <para>- if the rows can be exploded one-by-one without knowing the other rows, then using <see cref="ExplodeMutatorFluent.Explode(IFluentSequenceMutatorBuilder, ExplodeMutator)"/> is highly recommended.</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder ExplodeInMemory(this IFluentSequenceMutatorBuilder builder, InMemoryExplodeMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    /// <summary>
    /// Create any number of new rows based on the input rows.
    /// <para>- memory footprint is high because all rows are collected before the delegate is called</para>
    /// <para>- if the rows can be exploded one-by-one without knowing the other rows, then using <see cref="ExplodeMutatorFluent.Explode(IFluentSequenceMutatorBuilder, ExplodeMutator)"/> is highly recommended.</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder ExplodeInMemory(this IFluentSequenceMutatorBuilder builder, string name, InMemoryExplodeDelegate action)
    {
        return builder.AddMutator(new InMemoryExplodeMutator(builder.ProcessBuilder.Result.Context)
        {
            Name = name,
            Action = action,
        });
    }
}
