namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class BatchedCompareWithRowMutator : AbstractBatchedKeyBasedCrossMutator
    {
        public IRowEqualityComparer EqualityComparer { get; set; }
        public MatchAction MatchAndEqualsAction { get; set; }
        public MatchAction MatchButDifferentAction { get; set; }
        public NoMatchAction NoMatchAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        public BatchedCompareWithRowMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
            UseBatchKeys = false;
        }

        protected override void MutateRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed)
        {
            removeOriginal = false;
            processed = false;
        }

        protected override void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows)
        {
            var rightProcess = RightProcessCreator.Invoke(rows.ToArray());

            var lookup = new Dictionary<string, IRow>();
            var allRightRows = rightProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
            var rightRowCount = 0;
            foreach (var row in allRightRows)
            {
                rightRowCount++;
                var key = GetRightKey(row);
                if (string.IsNullOrEmpty(key))
                    continue;

                lookup[key] = row;
            }

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);

            foreach (var row in rows)
            {
                var key = GetLeftKey(row);

                var removeRow = false;
                if (key == null || !lookup.TryGetValue(key, out var match))
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
                    var isSame = EqualityComparer.Equals(row, match);
                    if (!isSame)
                    {
                        if (MatchButDifferentAction != null)
                        {
                            switch (MatchButDifferentAction.Mode)
                            {
                                case MatchMode.Remove:
                                    removeRow = true;
                                    break;
                                case MatchMode.Throw:
                                    var exception = new ProcessExecutionException(this, row, "no match");
                                    exception.Data.Add("Key", key);
                                    throw exception;
                                case MatchMode.Custom:
                                    MatchButDifferentAction.CustomAction.Invoke(this, row, match);
                                    break;
                            }
                        }
                    }
                    else if (MatchAndEqualsAction != null)
                    {
                        switch (MatchAndEqualsAction.Mode)
                        {
                            case MatchMode.Remove:
                                removeRow = true;
                                break;
                            case MatchMode.Throw:
                                var exception = new ProcessExecutionException(this, row, "match");
                                exception.Data.Add("Key", key);
                                throw exception;
                            case MatchMode.Custom:
                                MatchAndEqualsAction.CustomAction.Invoke(this, row, match);
                                break;
                        }
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

            if (MatchAndEqualsAction == null && NoMatchAction == null)
                throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction));

            if (MatchAndEqualsAction?.Mode == MatchMode.Custom && MatchAndEqualsAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(MatchAndEqualsAction) + "." + nameof(MatchAndEqualsAction.CustomAction));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            if (NoMatchAction != null && MatchAndEqualsAction != null && ((NoMatchAction.Mode == MatchMode.Remove && MatchAndEqualsAction.Mode == MatchMode.Remove) || (NoMatchAction.Mode == MatchMode.Throw && MatchAndEqualsAction.Mode == MatchMode.Throw)))
                throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction));

            if (EqualityComparer == null)
                throw new ProcessParameterNullException(this, nameof(EqualityComparer));
        }
    }
}