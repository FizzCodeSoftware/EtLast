namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;

/// <summary>
/// Input must be pre-grouped by key columns.
/// Group key generation is applied on the input rows on-the-fly. The collected group is processed when a new key is found.
/// - keeps the rows of a single group in memory
/// - uses very flexible <see cref="IMemoryAggregationOperation"/> which takes all rows in a group and generates the aggregate.
/// </summary>
public sealed class SortedMemoryAggregationMutator : AbstractMemoryAggregationMutator
{
    public SortedMemoryAggregationMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        var groupRows = new List<IReadOnlySlimRow>();
        string lastKey = null;

        netTimeStopwatch.Stop();
        var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
        netTimeStopwatch.Start();

        var success = true;

        var rowCount = 0;
        var ignoredRowCount = 0;
        var groupCount = 0;
        var aggregateCount = 0;
        while (!Context.CancellationTokenSource.IsCancellationRequested)
        {
            netTimeStopwatch.Stop();
            var finished = !enumerator.MoveNext();
            netTimeStopwatch.Start();
            if (finished)
                break;

            var row = enumerator.Current;

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

            rowCount++;
            var key = KeyGenerator.Invoke(row);
            if (key != lastKey)
            {
                lastKey = key;

                if (groupRows.Count > 0)
                {
                    var aggregates = new List<SlimRow>();
                    groupCount++;
                    try
                    {
                        Operation.TransformGroup(groupRows, () =>
                        {
                            var aggregate = new SlimRow
                            {
                                Tag = groupRows[0].Tag
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
                        var exception = new MemoryAggregationException(this, Operation, groupRows, ex);
                        Context.AddException(this, exception);
                        success = false;
                        break;
                    }

                    foreach (var groupRow in groupRows)
                    {
                        Context.SetRowOwner(groupRow as IRow, null);
                    }

                    groupRows.Clear();

                    foreach (var aggregate in aggregates)
                    {
                        aggregateCount++;
                        var aggregateRow = Context.CreateRow(this, aggregate);

                        netTimeStopwatch.Stop();
                        yield return aggregateRow;
                        netTimeStopwatch.Start();
                    }
                }
            }

            groupRows.Add(row);
        }

        if (success && groupRows.Count > 0)
        {
            var aggregates = new List<SlimRow>();
            groupCount++;
            try
            {
                Operation.TransformGroup(groupRows, () =>
                {
                    var aggregate = new SlimRow();

                    if (FixColumns != null)
                    {
                        foreach (var col in FixColumns)
                        {
                            aggregate[col.Key] = groupRows[0][col.Value ?? col.Key];
                        }
                    }

                    aggregates.Add(aggregate);
                    return aggregate;
                });
            }
            catch (Exception ex)
            {
                var exception = new MemoryAggregationException(this, Operation, groupRows, ex);
                Context.AddException(this, exception);
                success = false;
            }

            foreach (var groupRow in groupRows)
            {
                Context.SetRowOwner(groupRow as IRow, null);
            }

            groupRows.Clear();

            if (success)
            {
                foreach (var aggregate in aggregates)
                {
                    aggregateCount++;
                    var aggregateRow = Context.CreateRow(this, aggregate);

                    netTimeStopwatch.Stop();
                    yield return aggregateRow;
                    netTimeStopwatch.Start();
                }
            }
        }

        netTimeStopwatch.Stop();
        Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows, created {GroupCount} groups and created {AggregateCount} aggregates in {Elapsed}/{ElapsedWallClock}, ignored: {IgnoredRowCount}",
            rowCount, groupCount, aggregateCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed, ignoredRowCount);

        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class SortedMemoryAggregationMutatorFluent
{
    /// <summary>
    /// <para>- input must be ordered by group key</para>
    /// <para>- returns each aggregate right after a group is processed (stream-like behavior like most mutators)</para>
    /// <para>- if there is an ordering mismatch in the input then later appearances of a previously processed key will create new aggregated group(s)</para>
    /// <para>- memory footprint is very low because only rows of one group are collected before aggregation is executed on them</para>
    /// </summary>
    public static IFluentProcessMutatorBuilder AggregateOrdered(this IFluentProcessMutatorBuilder builder, SortedMemoryAggregationMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
