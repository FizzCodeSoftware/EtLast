namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    // almost the same as BatchedKeyTestMutator but it stores the row not just the keys
    public class BatchedValidateForeignKeyMutator : AbstractBatchedKeyBasedCrossMutator
    {
        public RowTestDelegate If { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Default value is 10.000
        /// </summary>
        public int CacheSizeLimit { get; set; } = 10000;

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public BatchedValidateForeignKeyMutator(IEtlContext context, string name, string topic)
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
                if (MatchAction != null && key != null && _lookup.TryGetValue(key, out var match))
                {
                    CounterCollection.IncrementCounter("served from cache", 1, true);

                    var removeRow = false;
                    switch (MatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            var exception = new ProcessExecutionException(this, row, "match");
                            exception.Data.Add("Key", key);
                            throw exception;
                        case MatchMode.Custom:
                            MatchAction.CustomAction.Invoke(this, row, match);
                            break;
                    }

                    if (removeRow)
                    {
                        Context.SetRowOwner(row, null);
                    }
                    else
                    {
                        yield return row;
                    }

                    continue;
                }

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

            var allRightRows = rightProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
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

            foreach (var row in batch)
            {
                var leftKey = GetLeftKey(row);

                var removeRow = false;
                if (leftKey == null || !_lookup.TryGetValue(leftKey, out var match))
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
                                exception.Data.Add("LeftKey", leftKey);
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
                            exception2.Data.Add("LeftKey", leftKey);
                            throw exception2;
                        case MatchMode.Custom:
                            MatchAction.CustomAction.Invoke(this, row, match);
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

            if (_lookup.Count >= CacheSizeLimit)
            {
                _lookup.Clear();
            }
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

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