namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Input can be unordered. Group key generation is applied on the input rows on-the-fly. Group rows are maintained on-the-fly using the current row.
    /// - discards input rows on-the-fly
    /// - keeps all grouped rows in memory (!)
    /// - uses restricted IContinuousAggregationOperation which takes the aggregated row + the actual row + the amount of rows already processed in the group
    ///   - sum, max, min, avg are trivial functions, but some others can be tricky
    ///  - 1 group always results 1 aggregated row
    /// </summary>
    public class ContinuousAggregationProcess : AbstractAggregationProcess
    {
        public IContinuousAggregationOperation Operation { get; set; }

        public ContinuousAggregationProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        private class AggregateRow
        {
            public IRow Row { get; set; }
            public int RowsInGroup { get; set; }
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;

            Operation.Prepare();

            if (GroupingColumns == null || GroupingColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(GroupingColumns));
            if (Operation == null)
                throw new ProcessParameterNullException(this, nameof(Operation));

            Context.Log(LogSeverity.Information, this, "started");

            var startedOn = Stopwatch.StartNew();
            var groups = new Dictionary<string, AggregateRow>();
            var rows = InputProcess.Evaluate(this);

            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;
                var key = GenerateKey(row);
                if (row.Flagged)
                    Context.LogRow(this, row, "aggregation group key generated: {GroupKey}", key);

                if (!groups.TryGetValue(key, out var aggregateRow))
                {
                    aggregateRow = new AggregateRow { Row = Context.CreateRow(row.ColumnCount) };
                    foreach (var column in GroupingColumns)
                    {
                        aggregateRow.Row.SetValue(column, row[column], this);
                    }

                    groups.Add(key, aggregateRow);
                }

                try
                {
                    TransformGroup(row, aggregateRow);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                    break;
                }

                aggregateRow.RowsInGroup++;
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}", rowCount, groups.Count, startedOn.Elapsed);

            var terminated = Context.CancellationTokenSource.IsCancellationRequested;
            if (terminated)
            {
                yield break;
            }

            foreach (var group in groups.Values)
            {
                yield return group.Row;
            }

            Operation.Shutdown();

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", groups.Count, startedOn.Elapsed);
        }

        private void TransformGroup(IRow row, AggregateRow aggregateRow)
        {
            try
            {
                Operation.TransformGroup(GroupingColumns, this, row, aggregateRow.Row, aggregateRow.RowsInGroup);
            }
            catch (EtlException) { throw; }
            catch (Exception ex) { throw new ContinuousAggregationOperationExecutionException(this, Operation, row, aggregateRow.Row, ex); }
        }
    }
}