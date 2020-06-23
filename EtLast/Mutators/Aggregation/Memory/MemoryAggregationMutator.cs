namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
    /// - keeps all input rows in memory (!)
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
            if (GroupingColumns == null || GroupingColumns.Count == 0)
                throw new ProcessParameterNullException(this, nameof(GroupingColumns));

            if (Operation == null)
                throw new ProcessParameterNullException(this, nameof(Operation));
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

                var aggregate = new SlimRow();
                foreach (var column in GroupingColumns)
                {
                    aggregate.SetValue(column.ToColumn, group[0][column.FromColumn]);
                }

                try
                {
                    Operation.TransformGroup(group, aggregate);
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

                aggregateCount++;
                var aggregateRow = Context.CreateRow(this, aggregate.Values);

                netTimeStopwatch.Stop();
                yield return aggregateRow;
                netTimeStopwatch.Start();
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "created {AggregateCount} aggregates in {Elapsed}/{ElapsedWallClock}",
                aggregateCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}