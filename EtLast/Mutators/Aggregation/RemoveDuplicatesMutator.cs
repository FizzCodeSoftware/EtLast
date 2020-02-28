namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    /// <summary>
    /// Input can be unordered.
    /// - discards input rows on-the-fly
    /// - keeps all aggregates in memory (!)
    /// - uses limited <see cref="IContinuousAggregationOperation"/> which takes the aggregate + the actual row + the amount of rows already processed in the group
    ///   - sum, max, min, avg are trivial functions, but some others can be tricky
    ///  - each group results 0 or 1 aggregate per group
    /// </summary>
    public class RemoveDuplicatesMutator : AbstractAggregationMutator
    {
        public RemoveDuplicatesMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (GroupingColumns == null || GroupingColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(GroupingColumns));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            Context.Log(LogSeverity.Information, this, "started");

            var returnedKeys = new HashSet<string>();

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndReleaseOwnership().GetEnumerator();
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
                if (!returnedKeys.Contains(key))
                {
                    var initialValues = GroupingColumns
                        .Select(column => new KeyValuePair<string, object>(column, row[column]));

                    var newRow = Context.CreateRow(this, initialValues);
                    netTimeStopwatch.Stop();
                    yield return newRow;
                    netTimeStopwatch.Start();

                    returnedKeys.Add(key);
                }
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and returned {ResultRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                rowCount, returnedKeys.Count, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}