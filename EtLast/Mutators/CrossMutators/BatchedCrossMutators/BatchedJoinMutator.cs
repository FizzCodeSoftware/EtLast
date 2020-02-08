namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class BatchedJoinMutator : AbstractBatchedKeyBasedCrossMutator
    {
        public RowTestDelegate If { get; set; }

        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public int BatchSize { get; set; } = 1000;

        public BatchedJoinMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var batch = new List<IRow>();
            var batchKeys = new List<string>();

            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    CounterCollection.IncrementCounter("ignored", 1);
                    yield return row;
                    continue;
                }

                CounterCollection.IncrementCounter("processed", 1);

                var key = GetLeftKey(row);
                batch.Add(row);
                batchKeys.Add(key);

                if (batchKeys.Count >= BatchSize)
                {
                    foreach (var r in ProcessBatch(batch))
                    {
                        yield return r;
                    }

                    batch.Clear();
                    batchKeys.Clear();
                }
            }

            if (batch.Count > 0)
            {
                foreach (var r in ProcessBatch(batch))
                {
                    yield return r;
                }

                batch.Clear();
            }
        }

        private IEnumerable<IRow> ProcessBatch(List<IRow> batch)
        {
            var rightProcess = RightProcessCreator.Invoke(batch.ToArray());

            Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", rightProcess.Name);

            var lookup = new Dictionary<string, List<IRow>>();
            var allRightRows = rightProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
            var rightRowCount = 0;
            foreach (var row in allRightRows)
            {
                rightRowCount++;
                var key = GetRightKey(row);
                if (string.IsNullOrEmpty(key))
                    continue;

                if (!lookup.TryGetValue(key, out var list))
                {
                    list = new List<IRow>();
                    lookup.Add(key, list);
                }

                list.Add(row);
            }

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);

            foreach (var row in batch)
            {
                var leftKey = GetLeftKey(row);
                List<IRow> rightRows = null;
                if (leftKey != null)
                    lookup.TryGetValue(leftKey, out rightRows);

                /*if (rightRows != null && RightRowFilter != null)
                {
                    rightRows = rightRows
                        .Where(rightRow => RightRowFilter.Invoke(row, rightRow))
                        .ToList();
                }*/

                var removeRow = false;
                if (rightRows?.Count > 0)
                {
                    if (rightRows.Count > 1)
                    {
                        for (var i = 1; i < rightRows.Count; i++)
                        {
                            var initialValues = row.Values.ToList();
                            foreach (var config in ColumnConfiguration)
                            {
                                config.Copy(rightRows[i], initialValues);
                            }

                            var newRow = Context.CreateRow(this, initialValues);

                            MatchCustomAction?.Invoke(this, newRow, rightRows[i]);
                            yield return newRow;
                        }
                    }

                    foreach (var config in ColumnConfiguration)
                    {
                        config.Copy(this, rightRows[0], row);
                    }

                    MatchCustomAction?.Invoke(this, row, rightRows[0]);
                }
                else if (NoMatchAction != null)
                {
                    switch (NoMatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            var exception = new ProcessExecutionException(this, row, "no match");
                            exception.Data.Add("LeftKey", leftKey);
                            throw exception;
                        case MatchMode.Custom:
                            NoMatchAction.CustomAction.Invoke(this, row);
                            break;
                    }
                }

                if (removeRow)
                {
                    Context.SetRowOwner(row, null);
                }
                else
                {
                    yield return row;
                }
            }

            lookup.Clear();
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
        }
    }
}