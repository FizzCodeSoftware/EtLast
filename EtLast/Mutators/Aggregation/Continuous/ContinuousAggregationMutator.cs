namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;

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
            init
            {
                //_operation?.SetProcess(null);

                _operation = value;
                _operation.SetProcess(this);
            }
        }

        public ContinuousAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (KeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(KeyGenerator));

            if (Operation == null)
                throw new ProcessParameterNullException(this, nameof(Operation));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            var aggregates = new Dictionary<string, ContinuousAggregate>();

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var rowCount = 0;
            var ignoredRowCount = 0;
            while (!Context.CancellationTokenSource.IsCancellationRequested)
            {
                netTimeStopwatch.Stop();
                var finished = !enumerator.MoveNext();
                netTimeStopwatch.Start();
                if (finished)
                    break;

                var row = enumerator.Current;

                var apply = false;
                try
                {
                    apply = If?.Invoke(row) != false;
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

                rowCount++;
                var key = KeyGenerator.Invoke(row);
                if (!aggregates.TryGetValue(key, out var aggregate))
                {
                    aggregate = new ContinuousAggregate();

                    if (FixColumns != null)
                    {
                        foreach (var column in FixColumns)
                        {
                            aggregate.ResultRow.SetValue(column.ToColumn, row[column.FromColumn]);
                        }
                    }

                    aggregates.Add(key, aggregate);
                }

                try
                {
                    Operation.TransformAggregate(row, aggregate);
                }
                catch (Exception ex)
                {
                    var exception = new ContinuousAggregationException(this, Operation, row, ex);
                    Context.AddException(this, exception);
                    break;
                }

                aggregate.RowsInGroup++;

                Context.SetRowOwner(row, null);
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}, ignored: {IgnoredRowCount}",
                rowCount, aggregates.Count, InvocationInfo.LastInvocationStarted.Elapsed, ignoredRowCount);

            foreach (var aggregate in aggregates.Values)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

                var row = Context.CreateRow(this, aggregate.ResultRow);

                netTimeStopwatch.Stop();
                yield return row;
                netTimeStopwatch.Start();
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "created {AggregateRowCount} aggregates in {Elapsed}/{ElapsedWallClock}",
                aggregates.Count, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            aggregates.Clear();

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
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
        public static IFluentProcessMutatorBuilder AggregateContinuously(this IFluentProcessMutatorBuilder builder, ContinuousAggregationMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}