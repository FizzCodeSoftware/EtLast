﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

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
        private IContinuousAggregationOperation _operation;

        public IContinuousAggregationOperation Operation
        {
            get => _operation;
            set
            {
                _operation?.SetProcess(null);

                _operation = value;
                _operation.SetProcess(this);
            }
        }

        public ContinuousAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        private class Aggregate
        {
            public IRow Row { get; set; }
            public int RowsInGroup { get; set; }
        }

        protected override void ValidateImpl()
        {
            if (GroupingColumns == null || GroupingColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(GroupingColumns));

            if (Operation == null)
                throw new ProcessParameterNullException(this, nameof(Operation));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            Context.Log(LogSeverity.Information, this, "continuous aggregation started");

            var aggregates = new Dictionary<string, Aggregate>();

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
                if (!aggregates.TryGetValue(key, out var aggregate))
                {
                    var initialValues = GroupingColumns
                        .Select(column => new KeyValuePair<string, object>(column, row[column]));

                    aggregate = new Aggregate
                    {
                        Row = Context.CreateRow(this, initialValues),
                    };

                    aggregates.Add(key, aggregate);
                }

                try
                {
                    try
                    {
                        Operation.TransformGroup(GroupingColumns, row, aggregate.Row, aggregate.RowsInGroup);
                    }
                    catch (EtlException) { throw; }
                    catch (Exception ex) { throw new ContinuousAggregationException(this, Operation, row, aggregate.Row, ex); }
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                    break;
                }

                aggregate.RowsInGroup++;

                Context.SetRowOwner(row, null);
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}",
                rowCount, aggregates.Count, InvocationInfo.LastInvocationStarted.Elapsed);

            foreach (var aggregate in aggregates.Values)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

                netTimeStopwatch.Stop();
                yield return aggregate.Row;
                netTimeStopwatch.Start();
            }

            Context.Log(LogSeverity.Debug, this, "created {AggregateRowCount} aggregates in {Elapsed}/{ElapsedWallClock}",
                aggregates.Count, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}