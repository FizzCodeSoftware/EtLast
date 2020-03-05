namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    /// <summary>
    /// Input can be unordered.
    /// - discards input rows on-the-fly
    /// - keeps already yielded row keys in memory (!)
    /// </summary>
    public class RemoveDuplicateRowsMutator : AbstractAggregationMutator
    {
        public RemoveDuplicateRowsMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (GroupingColumns == null || GroupingColumns.Count == 0)
                throw new ProcessParameterNullException(this, nameof(GroupingColumns));
        }

        protected override IEnumerable<IEtlRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
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
                        .Select(column => new KeyValuePair<string, object>(column.ToColumn, row[column.FromColumn]));

                    var newRow = Context.CreateRow(this, initialValues);
                    netTimeStopwatch.Stop();
                    yield return newRow;
                    netTimeStopwatch.Start();

                    returnedKeys.Add(key);
                }
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and returned {ResultRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                rowCount, returnedKeys.Count, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}