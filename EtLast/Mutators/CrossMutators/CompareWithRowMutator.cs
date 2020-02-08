namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class CompareWithRowMutator : AbstractKeyBasedCrossMutator
    {
        public RowTestDelegate If { get; set; }
        public IRowEqualityComparer EqualityComparer { get; set; }
        public MatchAction MatchAndEqualsAction { get; set; }
        public MatchAction MatchButDifferentAction { get; set; }
        public NoMatchAction NoMatchAction { get; set; }

        public CompareWithRowMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", RightProcess.Name);

            var lookup = new Dictionary<string, IRow>();
            var allRightRows = RightProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
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

                var leftKey = GetLeftKey(row);
                var removeRow = false;
                if (leftKey == null || !lookup.TryGetValue(leftKey, out var match))
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
                else
                {
                    var isSame = EqualityComparer.Equals(row, match);
                    if (isSame)
                    {
                        if (MatchAndEqualsAction != null)
                        {
                            switch (MatchAndEqualsAction.Mode)
                            {
                                case MatchMode.Remove:
                                    removeRow = true;
                                    break;
                                case MatchMode.Throw:
                                    var exception = new ProcessExecutionException(this, row, "match");
                                    exception.Data.Add("LeftKey", leftKey);
                                    throw exception;
                                case MatchMode.Custom:
                                    MatchAndEqualsAction.CustomAction.Invoke(this, row, match);
                                    break;
                            }
                        }
                    }
                    else if (MatchButDifferentAction != null)
                    {
                        switch (MatchButDifferentAction.Mode)
                        {
                            case MatchMode.Remove:
                                removeRow = true;
                                break;
                            case MatchMode.Throw:
                                var exception = new ProcessExecutionException(this, row, "no match");
                                exception.Data.Add("LeftKey", leftKey);
                                throw exception;
                            case MatchMode.Custom:
                                MatchButDifferentAction.CustomAction.Invoke(this, row, match);
                                break;
                        }
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

            if (MatchAndEqualsAction == null && NoMatchAction == null && MatchButDifferentAction == null)
                throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction) + " or " + nameof(MatchButDifferentAction));

            if (MatchAndEqualsAction?.Mode == MatchMode.Custom && MatchAndEqualsAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(MatchAndEqualsAction) + "." + nameof(MatchAndEqualsAction.CustomAction));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            if (NoMatchAction != null && MatchAndEqualsAction != null && ((NoMatchAction.Mode == MatchMode.Remove && MatchAndEqualsAction.Mode == MatchMode.Remove) || (NoMatchAction.Mode == MatchMode.Throw && MatchAndEqualsAction.Mode == MatchMode.Throw)))
                throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action mode: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction));

            if (EqualityComparer == null)
                throw new ProcessParameterNullException(this, nameof(EqualityComparer));
        }
    }
}