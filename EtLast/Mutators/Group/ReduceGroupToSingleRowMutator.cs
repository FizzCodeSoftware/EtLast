namespace FizzCode.EtLast;

public delegate IRow ReduceGroupToSingleRowDelegate(IProcess process, IReadOnlyList<IRow> groupRows);

/// <summary>
/// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
/// - keeps all input rows in memory (!)
/// </summary>
public sealed class ReduceGroupToSingleRowMutator : AbstractSequence, IMutator
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }

    public Func<IReadOnlyRow, string> KeyGenerator { get; init; }
    public ReduceGroupToSingleRowDelegate Selector { get; init; }

    /// <summary>
    /// Default false. Setting to true means the Selector won't be called for groups with a single row - which can improve performance and/or introduce side effects.
    /// </summary>
    public bool IgnoreSelectorForSingleRowGroups { get; init; }

    public ReduceGroupToSingleRowMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (KeyGenerator == null)
            throw new ProcessParameterNullException(this, nameof(KeyGenerator));

        if (Selector == null)
            throw new ProcessParameterNullException(this, nameof(Selector));
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        var groups = new Dictionary<string, object>();

        netTimeStopwatch.Stop();
        var enumerator = Input.TakeRowsAndTransferOwnership(this).GetEnumerator();
        netTimeStopwatch.Start();

        var mutatedRowCount = 0;
        var ignoredRowCount = 0;
        var resultRowCount = 0;
        while (!Context.CancellationToken.IsCancellationRequested)
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
            var key = KeyGenerator.Invoke(row);
            if (!groups.TryGetValue(key, out var group))
            {
                groups.Add(key, row);
            }
            else
            {
                if (group is not List<IRow> list)
                {
                    groups[key] = list = new List<IRow>();
                    list.Add(group as IRow);
                }

                list.Add(row);
            }
        }

        netTimeStopwatch.Start();

        Context.Log(LogSeverity.Debug, this, "evaluated {MutatedRowCount} of {TotalRowCount} rows and created {GroupCount} groups in {Elapsed}/{ElapsedWallClock}",
            mutatedRowCount, mutatedRowCount + ignoredRowCount, groups.Count, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

        var fakeList = new List<IRow>();

        foreach (var group in groups.Values)
        {
            if (Context.CancellationToken.IsCancellationRequested)
                break;

            var singleRow = group as IRow;

            if (IgnoreSelectorForSingleRowGroups && singleRow != null)
            {
                resultRowCount++;

                netTimeStopwatch.Stop();
                yield return singleRow;
                netTimeStopwatch.Start();

                continue;
            }

            IRow resultRow = null;

            List<IRow> list = null;
            if (singleRow != null)
            {
                fakeList.Clear();
                fakeList.Add(singleRow);
                list = fakeList;
            }
            else
            {
                list = group as List<IRow>;
            }

            try
            {
                resultRow = Selector.Invoke(this, list);
            }
            catch (Exception ex)
            {
                AddException(ex);
                break;
            }

            foreach (var row in list)
            {
                if (row != resultRow)
                    Context.SetRowOwner(row, null);
            }

            if (resultRow != null)
            {
                resultRowCount++;

                netTimeStopwatch.Stop();
                yield return resultRow;
                netTimeStopwatch.Start();
            }
        }

        Context.Log(LogSeverity.Debug, this, "returned {ResultRowCount} rows in {Elapsed}/{ElapsedWallClock}",
            resultRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
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
public static class ReduceGroupToSingleRowMutatorFluent
{
    /// <summary>
    /// Organize input rows into groups and activates a selector which must select zero or one row from the group to be kept. All other rows of the group are discared.
    /// <para>- input can be unordered</para>
    /// <para>- returns all selected rows at once when everything is processed. Memory footprint is high because all rows in all groups are collected before selection</para>
    /// <para>- if the input is ordered then <see cref="SortedReduceGroupToSingleRowMutatorFluent.ReduceGroupToSingleRowOrdered(IFluentSequenceMutatorBuilder, SortedReduceGroupToSingleRowMutator)"/> should be used for much lower memory footprint and stream-like behavior</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder ReduceGroupToSingleRow(this IFluentSequenceMutatorBuilder builder, ReduceGroupToSingleRowMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
