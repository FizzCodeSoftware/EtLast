namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class BatchedKeyTestMutator : AbstractBatchedKeyBasedCrossMutator
    {
        public NoMatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Default value is 100.000
        /// </summary>
        public int CacheSizeLimit { get; set; } = 100000;

        private readonly HashSet<string> _lookup = new HashSet<string>();

        public BatchedKeyTestMutator(ITopic topic, string name)
            : base(topic, name)
        {
            UseBatchKeys = true;
        }

        protected override void MutateRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed)
        {
            removeOriginal = false;

            var key = GetLeftKey(row);
            if (MatchAction != null && key != null && _lookup.Contains(key))
            {
                processed = true;
                CounterCollection.IncrementCounter("served from cache", 1, true);

                switch (MatchAction.Mode)
                {
                    case MatchMode.Remove:
                        removeOriginal = true;
                        break;
                    case MatchMode.Throw:
                        var exception = new ProcessExecutionException(this, row, "match");
                        exception.Data.Add("Key", key);
                        throw exception;
                    case MatchMode.Custom:
                        MatchAction.CustomAction.Invoke(this, row, row);
                        break;
                }

                if (!removeOriginal)
                {
                    mutatedRows.Add(row);
                }
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

                _lookup.Add(key);
            }

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, _lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);

            foreach (var row in rows)
            {
                var key = GetLeftKey(row);

                var removeRow = false;
                if (key == null || !_lookup.Contains(key))
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
                else if (MatchAction != null)
                {
                    switch (MatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            var exception2 = new ProcessExecutionException(this, row, "match");
                            exception2.Data.Add("LeftKey", key);
                            throw exception2;
                        case MatchMode.Custom:
                            MatchAction.CustomAction.Invoke(this, row, row);
                            break;
                    }
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

            if (MatchAction == null && NoMatchAction == null)
                throw new InvalidProcessParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));

            if (MatchAction?.Mode == MatchMode.Custom && MatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(MatchAction) + "." + nameof(MatchAction.CustomAction));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            if (NoMatchAction != null && MatchAction != null && ((NoMatchAction.Mode == MatchMode.Remove && MatchAction.Mode == MatchMode.Remove) || (NoMatchAction.Mode == MatchMode.Throw && MatchAction.Mode == MatchMode.Throw)))
                throw new InvalidProcessParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));
        }
    }
}