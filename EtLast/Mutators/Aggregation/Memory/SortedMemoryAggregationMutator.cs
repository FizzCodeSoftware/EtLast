namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Input must be pre-grouped by key columns.
    /// Group key generation is applied on the input rows on-the-fly. The collected group is processed when a new key is found.
    /// - keeps the rows of a single group in memory
    /// - uses very flexible <see cref="IMemoryAggregationOperation"/> which takes all rows in a group and generates the aggregate.
    /// </summary>
    public class SortedMemoryAggregationMutator : AbstractMemoryAggregationMutator
    {
        public SortedMemoryAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            var group = new List<IReadOnlySlimRow>();
            string lastKey = null;

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var success = true;

            var rowCount = 0;
            var aggregateCount = 0;
            while (!Context.CancellationTokenSource.IsCancellationRequested)
            {
                netTimeStopwatch.Stop();
                var finished = !enumerator.MoveNext();
                netTimeStopwatch.Start();
                if (finished)
                    break;

                var row = enumerator.Current;
                rowCount++;
                var key = GetKey(row);
                if (key != lastKey)
                {
                    lastKey = key;

                    if (group.Count > 0)
                    {
                        var aggregates = new List<SlimRow>();

                        try
                        {
                            Operation.TransformGroup(group, () =>
                            {
                                var aggregate = new SlimRow();
                                foreach (var column in GroupingColumns)
                                {
                                    aggregate.SetValue(column.ToColumn, group[0][column.FromColumn]);
                                }

                                aggregates.Add(aggregate);
                                return aggregate;
                            });
                        }
                        catch (Exception ex)
                        {
                            var exception = new MemoryAggregationException(this, Operation, group, ex);
                            Context.AddException(this, exception);
                            success = false;
                            break;
                        }

                        foreach (var groupRow in group)
                        {
                            Context.SetRowOwner(groupRow as IRow, null);
                        }

                        group.Clear();

                        foreach (var aggregate in aggregates)
                        {
                            aggregateCount++;
                            var aggregateRow = Context.CreateRow(this, aggregate.Values);

                            netTimeStopwatch.Stop();
                            yield return aggregateRow;
                            netTimeStopwatch.Start();
                        }
                    }
                }

                group.Add(row);
            }

            if (success && group.Count > 0)
            {
                var aggregates = new List<SlimRow>();

                try
                {
                    Operation.TransformGroup(group, () =>
                    {
                        var aggregate = new SlimRow();
                        foreach (var column in GroupingColumns)
                        {
                            aggregate.SetValue(column.ToColumn, group[0][column.FromColumn]);
                        }

                        aggregates.Add(aggregate);
                        return aggregate;
                    });
                }
                catch (Exception ex)
                {
                    var exception = new MemoryAggregationException(this, Operation, group, ex);
                    Context.AddException(this, exception);
                    success = false;
                }

                foreach (var groupRow in group)
                {
                    Context.SetRowOwner(groupRow as IRow, null);
                }

                group.Clear();

                if (success)
                {
                    foreach (var aggregate in aggregates)
                    {
                        aggregateCount++;
                        var aggregateRow = Context.CreateRow(this, aggregate.Values);

                        netTimeStopwatch.Stop();
                        yield return aggregateRow;
                        netTimeStopwatch.Start();
                    }
                }
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}",
                rowCount, group.Count, InvocationInfo.LastInvocationStarted.Elapsed);

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "created {AggregateCount} aggregates in {Elapsed}/{ElapsedWallClock}",
                aggregateCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}