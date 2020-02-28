﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
    /// - keeps all input rows in memory (!)
    /// - keeps all aggregates in memory (!)
    /// - uses very flexible <see cref="IMemoryAggregationOperation"/> which takes all rows in a group and generates the aggregate.
    ///  - each group results 0 or 1 aggregate per group
    /// </summary>
    public class MemoryAggregationMutator : AbstractAggregationMutator
    {
        private IMemoryAggregationOperation _operation;

        public IMemoryAggregationOperation Operation
        {
            get => _operation;
            set
            {
                _operation?.SetProcess(null);

                _operation = value;
                _operation.SetProcess(this);
            }
        }

        public MemoryAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
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
            Context.Log(LogSeverity.Information, this, "unordered aggregation started");

            var groups = new Dictionary<string, List<IRow>>();

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
                    list = new List<IRow>();
                    groups.Add(key, list);
                }

                list.Add(row);
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}",
                rowCount, groups.Count, InvocationInfo.LastInvocationStarted.Elapsed);

            var aggregateRowCount = 0;
            foreach (var group in groups.Values)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

                IRow aggregate;
                try
                {
                    try
                    {
                        aggregate = Operation.TransformGroup(GroupingColumns, group);
                    }
                    catch (EtlException) { throw; }
                    catch (Exception ex) { throw new MemoryAggregationException(this, Operation, group, ex); }
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                    break;
                }

                foreach (var row in group)
                {
                    Context.SetRowOwner(row, null);
                }

                if (aggregate != null)
                {
                    aggregateRowCount++;
                    netTimeStopwatch.Stop();
                    yield return aggregate;
                    netTimeStopwatch.Start();
                }
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "created {AggregateRowCount} aggregates in {Elapsed}/{ElapsedWallClock}",
                aggregateRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}