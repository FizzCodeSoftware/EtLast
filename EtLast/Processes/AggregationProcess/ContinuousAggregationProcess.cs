namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

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

        public ContinuousAggregationProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        private class AggregateRow
        {
            public IRow Row { get; set; }
            public int RowsInGroup { get; set; }
        }

        public override void ValidateImpl()
        {
            if (GroupingColumns == null || GroupingColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(GroupingColumns));

            if (Operation == null)
                throw new ProcessParameterNullException(this, nameof(Operation));
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Operation.Prepare();

            Context.Log(LogSeverity.Information, this, "continuous aggregation started");

            var aggregateRows = new Dictionary<string, AggregateRow>();
            var rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership(this);

            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;
                var key = GenerateKey(row);
                if (!aggregateRows.TryGetValue(key, out var aggregateRow))
                {
                    var initialValues = GroupingColumns.Select(column => new KeyValuePair<string, object>(column, row[column]));

                    aggregateRow = new AggregateRow
                    {
                        Row = Context.CreateRow(this, initialValues),
                    };

                    aggregateRows.Add(key, aggregateRow);
                }

                try
                {
                    try
                    {
                        Operation.TransformGroup(GroupingColumns, this, row, aggregateRow.Row, aggregateRow.RowsInGroup);
                    }
                    catch (EtlException) { throw; }
                    catch (Exception ex) { throw new ContinuousAggregationOperationExecutionException(this, Operation, row, aggregateRow.Row, ex); }
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex, Operation);
                    break;
                }

                aggregateRow.RowsInGroup++;

                Context.SetRowOwner(row, null, null);
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}", rowCount, aggregateRows.Count, LastInvocation.Elapsed);

            if (Context.CancellationTokenSource.IsCancellationRequested)
                yield break;

            foreach (var aggregateRow in aggregateRows.Values)
            {
                yield return aggregateRow.Row;
            }

            Operation.Shutdown();

            Context.Log(LogSeverity.Debug, this, "finished and returned {GroupCount} groups in {Elapsed}", aggregateRows.Count, LastInvocation.Elapsed);
        }
    }
}