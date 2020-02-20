namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class BatchedExpandMutator : AbstractBatchedKeyBasedCrossMutator
    {
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Default value is 10.000
        /// </summary>
        public int CacheSizeLimit { get; set; } = 10000;

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public BatchedExpandMutator(IEtlContext context, string name, string topic)
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

            var key = GetLeftKey(row);
            if (key != null && _lookup.TryGetValue(key, out var match))
            {
                processed = true;
                CounterCollection.IncrementCounter("served from cache", 1, true);

                ColumnCopyConfiguration.CopyManyToRowStage(match, row, ColumnConfiguration);
                row.ApplyStaging(this);

                MatchCustomAction?.Invoke(this, row, match);

                mutatedRows.Add(row);
            }
            else
            {
                processed = false;
            }
        }

        protected override void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows)
        {
            var rightProcess = RightProcessCreator.Invoke(rows.ToArray());

            var allRightRows = rightProcess.Evaluate(this).TakeRowsAndReleaseOwnership();
            var rightRowCount = 0;
            foreach (var row in allRightRows)
            {
                rightRowCount++;
                var key = GetRightKey(row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup[key] = row;
            }

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, _lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);

            foreach (var row in rows)
            {
                var key = GetLeftKey(row);

                var removeRow = false;
                if (key == null || !_lookup.TryGetValue(key, out var match))
                {
                    if (NoMatchAction != null)
                    {
                        switch (NoMatchAction.Mode)
                        {
                            case MatchMode.Remove:
                                removeRow = true;
                                break;
                            case MatchMode.Throw:
                                var exception = new ProcessExecutionException(this, row, "no match");
                                exception.Data.Add("LeftKey", key);
                                throw exception;
                            case MatchMode.Custom:
                                NoMatchAction.CustomAction.Invoke(this, row);
                                break;
                        }
                    }
                }
                else
                {
                    ColumnCopyConfiguration.CopyManyToRowStage(match, row, ColumnConfiguration);
                    row.ApplyStaging(this);

                    MatchCustomAction?.Invoke(this, row, match);
                }

                if (removeRow)
                    removedRows.Add(row);
                else
                    mutatedRows.Add(row);
            }

            if (_lookup.Count >= CacheSizeLimit)
            {
                _lookup.Clear();
            }
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