namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class BatchedJoinMutator : AbstractBatchedKeyBasedCrossMutator
    {
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        public BatchedJoinMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
            UseBatchKeys = true;
        }

        protected override string GetBatchKey(IRow row)
        {
            return GetLeftKey(row);
        }

        protected override void MutateRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed)
        {
            removeOriginal = false;
            processed = false;
        }

        protected override void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows)
        {
            var rightProcess = RightProcessCreator.Invoke(rows.ToArray());

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

            foreach (var row in rows)
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
                    removeRow = true;
                    foreach (var rightRow in rightRows)
                    {
                        var initialValues = new Dictionary<string, object>(row.Values);
                        ColumnCopyConfiguration.CopyMany(rightRow, initialValues, ColumnConfiguration);

                        var newRow = Context.CreateRow(this, initialValues);

                        MatchCustomAction?.Invoke(this, newRow, rightRow);
                        mutatedRows.Add(newRow);
                    }
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
                    removedRows.Add(row);
                else
                    mutatedRows.Add(row);
            }

            lookup.Clear();
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
        }
    }
}