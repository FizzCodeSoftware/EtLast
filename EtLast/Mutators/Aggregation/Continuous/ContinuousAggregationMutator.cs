namespace FizzCode.EtLast;

/// <summary>
/// Input can be unordered. Group key generation is applied on the input rows on-the-fly. Aggregates are maintained on-the-fly using the current row.
/// - discards input rows on-the-fly
/// - keeps all aggregates in memory (!)
/// - uses limited <see cref="IContinuousAggregationOperation"/> which takes the aggregate + the actual row + the amount of rows already processed in the group
///   - sum, max, min, avg are trivial functions, but some others can be tricky
///  - each group results 0 or 1 aggregate per group
/// </summary>
public class ContinuousAggregationMutator : AbstractAggregationMutator
{
    public required IContinuousAggregationOperation Operation { get; init; }

    public ContinuousAggregationMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (Operation == null)
            throw new ProcessParameterNullException(this, nameof(Operation));
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        Dictionary<string, ContinuousAggregate> aggregates = null;
        ContinuousAggregate singleAggregate = null;
        if (KeyGenerator != null)
            aggregates = new Dictionary<string, ContinuousAggregate>();

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
            ContinuousAggregate aggregate = null;

            if (KeyGenerator != null)
            {
                var key = KeyGenerator.Invoke(row);
                aggregates.TryGetValue(key, out aggregate);

                if (aggregate == null)
                {
                    aggregate = new ContinuousAggregate(row.Tag);

                    if (FixColumns != null)
                    {
                        foreach (var column in FixColumns)
                        {
                            aggregate.ResultRow[column.Key] = row[column.Value ?? column.Key];
                        }
                    }

                    aggregates.Add(key, aggregate);
                }
            }
            else
            {
                aggregate = singleAggregate;
                if (aggregate == null)
                {
                    singleAggregate = aggregate = new ContinuousAggregate(row.Tag);

                    if (FixColumns != null)
                    {
                        foreach (var column in FixColumns)
                        {
                            aggregate.ResultRow[column.Key] = row[column.Value ?? column.Key];
                        }
                    }
                }
            }

            try
            {
                Operation.TransformAggregate(row, aggregate);
            }
            catch (Exception ex)
            {
                FlowState.AddException(this, new ContinuousAggregationException(this, Operation, row, ex));
                break;
            }

            aggregate.RowsInGroup++;

            Context.SetRowOwner(row, null);
        }

        netTimeStopwatch.Start();

        if (aggregates != null)
        {
            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups, ignored: {IgnoredRowCount}",
                rowCount, aggregates.Count, ignoredRowCount);

            foreach (var aggregate in aggregates.Values)
            {
                if (FlowState.IsTerminating)
                    break;

                var row = Context.CreateRow(this, aggregate.ResultRow);

                netTimeStopwatch.Stop();
                yield return row;
                netTimeStopwatch.Start();
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "created {AggregateRowCount} aggregates",
                aggregates.Count);

            aggregates.Clear();
        }
        else if (singleAggregate != null)
        {
            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created 1 aggregate, ignored: {IgnoredRowCount}",
                rowCount, ignoredRowCount);

            var row = Context.CreateRow(this, singleAggregate.ResultRow);

            netTimeStopwatch.Stop();
            yield return row;

            Context.Log(LogSeverity.Debug, this, "created a single aggregate");
        }
        else
        {
            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created a 0 aggregates, ignored: {IgnoredRowCount}",
                rowCount, ignoredRowCount);
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ContinuousAggregationMutatorFluent
{
    /// <summary>
    /// <para>- input can be unordered</para>
    /// <para>- returns all aggregates at once when everything is processed (blocks execution)</para>
    /// <para>- memory footprint depends on input (only the aggregates of all groups are stored in memory during evaluation)</para>
    /// <para>- the usable operations are slightly limited</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder AggregateContinuously(this IFluentSequenceMutatorBuilder builder, ContinuousAggregationMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}