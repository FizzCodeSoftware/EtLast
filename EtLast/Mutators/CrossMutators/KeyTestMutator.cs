namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class KeyTestMutator : AbstractKeyBasedCrossMutator
    {
        public NoMatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }
        private HashSet<string> _lookup;

        public KeyTestMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void StartMutator()
        {
            Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", RightProcess.Name);

            _lookup = new HashSet<string>();
            var allRightRows = RightProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
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

        }

        protected override void CloseMutator()
        {
            _lookup.Clear();
            _lookup = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var leftKey = GetLeftKey(row);

            var removeRow = false;
            if (leftKey == null || !_lookup.Contains(leftKey))
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
                        MatchAction.CustomAction.Invoke(this, row, row);
                        break;
                }
            }

            if (!removeRow)
                yield return row;
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