namespace FizzCode.EtLast;

/// <summary>
/// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
/// - keeps all input rows in memory (!)
/// - uses very flexible <see cref="IMemoryAggregationOperation"/> which takes all rows in a group and generates the aggregate.
/// </summary>
public sealed class MemoryAggregationMutator : AbstractMemoryAggregationMutator
{
    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        var groups = new Dictionary<string, List<IReadOnlySlimRow>>();
        var groupCount = 0;

        netTimeStopwatch.Stop();
        var enumerator = Input.TakeRowsAndTransferOwnership(this).GetEnumerator();
        netTimeStopwatch.Start();

        var rowCount = 0;
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

            rowCount++;
            var key = KeyGenerator.Invoke(row);
            if (!groups.TryGetValue(key, out var list))
            {
                list = [];
                groups.Add(key, list);
                groupCount++;
            }

            list.Add(row);
        }

        netTimeStopwatch.Start();

        var aggregateCount = 0;
        var aggregates = new List<SlimRow>();
        foreach (var groupRows in groups.Values)
        {
            if (FlowState.IsTerminating)
                break;

            try
            {
                Operation.TransformGroup(groupRows, () =>
                {
                    var aggregate = new SlimRow
                    {
                        Tag = groupRows[0].Tag,
                    };

                    if (FixColumns != null)
                    {
                        foreach (var column in FixColumns)
                        {
                            aggregate[column.Key] = groupRows[0][column.Value ?? column.Key];
                        }
                    }

                    aggregates.Add(aggregate);
                    return aggregate;
                });
            }
            catch (Exception ex)
            {
                FlowState.AddException(this, new MemoryAggregationException(this, Operation, groupRows, ex));
                break;
            }

            foreach (var row in groupRows)
                (row as IRow)?.SetOwner(null);

            foreach (var aggregate in aggregates)
            {
                aggregateCount++;
                var aggregateRow = Context.CreateRow(this, aggregate);

                netTimeStopwatch.Stop();
                yield return aggregateRow;
                netTimeStopwatch.Start();
            }

            groupRows.Clear();
            aggregates.Clear();
        }

        groups.Clear();

        Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows, created {GroupCount} groups and created {AggregateCount} aggregates, ignored: {IgnoredRowCount}",
            rowCount, groupCount, aggregateCount, ignoredRowCount);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class MemoryAggregationMutatorFluent
{
    /// <summary>
    /// <para>- input can be unordered</para>
    /// <para>- returns all aggregates at once when everything is processed (blocks execution)</para>
    /// <para>- memory footprint is high because all rows in all groups are collected before aggregation</para>
    /// <para>- if the input is ordered then <see cref="SortedMemoryAggregationMutatorFluent.AggregateOrdered(IFluentSequenceMutatorBuilder, SortedMemoryAggregationMutator)"/> should be used for much lower memory footprint and stream-like behavior</para>
    /// <para>- if the input is unordered but only basic operations are used then <see cref="ContinuousAggregationMutatorFluent.AggregateContinuously(IFluentSequenceMutatorBuilder, ContinuousAggregationMutator)"/> should be used</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder Aggregate(this IFluentSequenceMutatorBuilder builder, MemoryAggregationMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
