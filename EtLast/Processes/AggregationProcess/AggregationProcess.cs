namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
    /// - keeps all input rows in memory (!)
    /// - keeps all grouped rows in memory (!)
    /// - uses very flexible IAggregationOperation which takes all rows in a group and generates 0..N "group rows"
    ///  - 1 group results 0..N aggregated row
    /// </summary>
    public class AggregationProcess : AbstractAggregationProcess
    {
        private IAggregationOperation _operation;

        public IAggregationOperation Operation
        {
            get => _operation;
            set
            {
                _operation?.SetProcess(null);

                _operation = value;
                _operation.SetProcess(this);
            }
        }

        public AggregationProcess(ITopic topic, string name)
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

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "unordered aggregation started");

            var groups = new Dictionary<string, List<IRow>>();
            var rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership();

            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;
                var key = GenerateKey(row);
                if (!groups.TryGetValue(key, out var list))
                {
                    list = new List<IRow>();
                    groups.Add(key, list);
                }

                list.Add(row);
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}", rowCount, groups.Count, LastInvocationStarted.Elapsed);

            if (Context.CancellationTokenSource.IsCancellationRequested)
                yield break;

            var resultCount = 0;
            foreach (var group in groups.Values)
            {
                IRow aggregateRow;
                try
                {
                    try
                    {
                        aggregateRow = Operation.TransformGroup(GroupingColumns, group);
                    }
                    catch (EtlException) { throw; }
                    catch (Exception ex) { throw new AggregationOperationExecutionException(this, Operation, group, ex); }
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

                if (aggregateRow != null)
                {
                    resultCount++;
                    yield return aggregateRow;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, LastInvocationStarted.Elapsed);
        }
    }
}