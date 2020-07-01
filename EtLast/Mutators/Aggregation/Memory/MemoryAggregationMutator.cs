namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
    /// - keeps all input rows in memory (!)
    /// - uses very flexible <see cref="IMemoryAggregationOperation"/> which takes all rows in a group and generates the aggregate.
    /// </summary>
    public class MemoryAggregationMutator : AbstractMemoryAggregationMutator
    {
        public MemoryAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            var groups = new Dictionary<string, List<IReadOnlySlimRow>>();

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var rowCount = 0;
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
                if (!groups.TryGetValue(key, out var list))
                {
                    list = new List<IReadOnlySlimRow>();
                    groups.Add(key, list);
                }

                list.Add(row);
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}",
                rowCount, groups.Count, InvocationInfo.LastInvocationStarted.Elapsed);

            var aggregateCount = 0;
            foreach (var group in groups.Values)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

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
                    break;
                }

                foreach (var row in group)
                {
                    Context.SetRowOwner(row as IRow, null);
                }

                foreach (var aggregate in aggregates)
                {
                    aggregateCount++;
                    var aggregateRow = Context.CreateRow(this, aggregate.Values);

                    netTimeStopwatch.Stop();
                    yield return aggregateRow;
                    netTimeStopwatch.Start();
                }
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "created {AggregateCount} aggregates in {Elapsed}/{ElapsedWallClock}",
                aggregateCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}