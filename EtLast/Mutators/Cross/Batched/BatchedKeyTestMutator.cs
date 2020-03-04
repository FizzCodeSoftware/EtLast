namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class BatchedKeyTestMutator : AbstractBatchedCrossMutator
    {
        public NoMatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }

        /// <summary>
        /// Default true. Setting to false results to significantly less memory usage.
        /// </summary>
        public bool MatchActionContainsMatch { get; set; } = true;

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Default value is 100.000
        /// </summary>
        public int CacheSizeLimit { get; set; } = 100000;

        private ICountableLookup _lookup;

        public BatchedKeyTestMutator(ITopic topic, string name)
            : base(topic, name)
        {
            UseBatchKeys = true;
        }

        protected override void StartMutator()
        {
            _lookup = MatchActionContainsMatch
                ? (ICountableLookup)new RowLookup()
                : new CountableOnlyLookup();

            base.StartMutator();
        }

        protected override void CloseMutator()
        {
            _lookup.Clear();

            base.CloseMutator();
        }

        protected override void MutateSingleRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed)
        {
            removeOriginal = false;

            if (MatchAction != null)
            {
                var key = GeneratorRowKey(row);
                var matchCount = _lookup.GetRowCountByKey(key);
                if (matchCount > 0)
                {
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
                            IRow match = null;
                            if (MatchActionContainsMatch)
                            {
                                match = (_lookup as RowLookup).GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(this, row, match);
                            break;
                    }

                    if (!removeOriginal)
                    {
                        mutatedRows.Add(row);
                    }

                    processed = true;
                    return;
                }
            }

            processed = false;
        }

        protected override void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows)
        {
            LookupBuilder.Append(_lookup, this, rows.ToArray());

            foreach (var row in rows)
            {
                var key = GeneratorRowKey(row);

                var removeRow = false;
                if (_lookup.GetRowCountByKey(key) == 0)
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
                                exception.Data.Add("Key", key);
                                throw exception;
                            case MatchMode.Custom:
                                NoMatchAction.InvokeCustomAction(this, row);
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
                            exception2.Data.Add("Key", key);
                            throw exception2;
                        case MatchMode.Custom:
                            IRow match = null;
                            if (MatchActionContainsMatch)
                            {
                                match = (_lookup as RowLookup).GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(this, row, match);
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