namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
    /// - keeps all input rows in memory (!)
    /// - keeps all grouped rows in memory (!)
    /// - uses very flexible IAggregationOperation which takes all rows in a group and generates 0..N "group rows"
    ///  - 1 group results 0..N aggregated row
    /// </summary>
    public class UnorderedAggregationProcess : AbstractAggregationProcess
    {
        private IAggregationOperation _operation;

        public IAggregationOperation Operation
        {
            get => _operation;
            set
            {
                _operation = value;
                _operation.SetProcess(this);
                _operation.SetParent(0);
            }
        }

        public UnorderedAggregationProcess(IEtlContext context, string name)
            : base(context, name)
        {
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
            var groups = new Dictionary<string, List<IRow>>();
            var rows = InputProcess.Evaluate(this);

            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;
                var key = GenerateKey(row);
                if (row.Flagged)
                    Context.LogRow(this, row, "aggregation group key generated: {GroupKey}", key);

                if (!groups.TryGetValue(key, out var list))
                {
                    list = new List<IRow>();
                    groups.Add(key, list);
                }

                list.Add(row);
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}", rowCount, groups.Count, startedOn.Elapsed);

            var terminated = Context.CancellationTokenSource.IsCancellationRequested;
            if (terminated)
            {
                yield break;
            }

            var resultCount = 0;
            foreach (var group in groups.Values)
            {
                IEnumerable<IRow> groupResult;
                try
                {
                    groupResult = TransformGroup(group);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                    break;
                }

                foreach (var resultRow in groupResult)
                {
                    resultCount++;
                    yield return resultRow;
                }
            }

            Operation.Shutdown();

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, startedOn.Elapsed);
        }

        private IEnumerable<IRow> TransformGroup(List<IRow> group)
        {
            IEnumerable<IRow> groupResult;
            try
            {
                groupResult = Operation.TransformGroup(GroupingColumns, this, group);
            }
            catch (EtlException) { throw; }
            catch (Exception ex) { throw new AggregationOperationExecutionException(this, Operation, group, ex); }

            return groupResult;
        }
    }
}