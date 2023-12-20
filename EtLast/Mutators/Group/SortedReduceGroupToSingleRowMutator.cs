namespace FizzCode.EtLast;

/// <summary>
/// Input must be pre-grouped by key columns.
/// Group key generation is applied on the input rows on-the-fly. The collected group is processed when a new key is found.
/// - keeps all row keys in memory (!)
/// </summary>
public sealed class SortedReduceGroupToSingleRowMutator : AbstractSequence, IMutator
{
    public ISequence Input { get; set; }
    public RowTestDelegate RowFilter { get; set; }
    public RowTagTestDelegate RowTagFilter { get; set; }

    [ProcessParameterMustHaveValue]
    public required Func<IReadOnlyRow, string> KeyGenerator { get; init; }

    [ProcessParameterMustHaveValue]
    public required ReduceGroupToSingleRowDelegate Selector { get; init; }

    /// <summary>
    /// Default false. Setting to true means the Selector won't be called for groups with a single row - which can improve performance and/or introduce side effects.
    /// </summary>
    public bool IgnoreSelectorForSingleRowGroups { get; init; }

    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        var group = new List<IRow>();
        string lastKey = null;

        netTimeStopwatch.Stop();
        var enumerator = Input.TakeRowsAndTransferOwnership(this).GetEnumerator();
        netTimeStopwatch.Start();

        var mutatedRowCount = 0;
        var ignoredRowCount = 0;
        var resultRowCount = 0;
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
            var key = KeyGenerator.Invoke(row);
            if (key != lastKey)
            {
                lastKey = key;

                var groupRow = ReduceGroup(group);

                if (groupRow != null)
                {
                    resultRowCount++;
                    netTimeStopwatch.Stop();
                    yield return groupRow;
                    netTimeStopwatch.Start();
                }
            }

            if (!FlowState.IsTerminating)
            {
                group.Add(row);
            }
        }

        netTimeStopwatch.Start();

        if (!FlowState.IsTerminating && group.Count > 0)
        {
            var groupRow = ReduceGroup(group);
            if (groupRow != null)
            {
                resultRowCount++;
                netTimeStopwatch.Stop();
                yield return groupRow;
                netTimeStopwatch.Start();
            }
        }

        Context.Log(LogSeverity.Debug, this, "evaluated {MutatedRowCount} of {TotalRowCount} rows and returned {ResultRowCount} rows",
            mutatedRowCount, mutatedRowCount + ignoredRowCount, resultRowCount);
    }

    private IRow ReduceGroup(List<IRow> group)
    {
        if (group.Count == 0)
            return null;

        IRow resultRow = null;
        if (group.Count != 1 || !IgnoreSelectorForSingleRowGroups)
        {
            try
            {
                resultRow = Selector.Invoke(this, group);
            }
            catch (Exception ex)
            {
                FlowState.AddException(this, ex);
                return null;
            }

            foreach (var groupRow in group)
            {
                if (groupRow != resultRow)
                    groupRow.SetOwner(null);
            }
        }
        else
        {
            resultRow = group[0];
        }

        group.Clear();
        return resultRow;
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
public static class SortedReduceGroupToSingleRowMutatorFluent
{
    /// <summary>
    /// Organize input rows into groups and activates a selector which must select zero or one row from the group to be kept. All other rows of the group are discared.
    /// <para>- input must be ordered by group key</para>
    /// <para>- returns each selected row right after a group is processed (stream-like behavior like most mutators)</para>
    /// <para>- if there is an ordering mismatch in the input then later appearances of a previously processed key will create new group(s) and selection will be executed on the new group again</para>
    /// <para>- memory footprint is very low because only rows of one group are collected before selection is executed on them</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder ReduceGroupToSingleRowOrdered(this IFluentSequenceMutatorBuilder builder, SortedReduceGroupToSingleRowMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
