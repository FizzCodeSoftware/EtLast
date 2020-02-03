namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class CompareWithRowOperation : AbstractKeyBasedCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public MatchAction MatchAction { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchAction NotSameAction { get; set; }
        public IRowEqualityComparer EqualityComparer { get; set; }

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public override void Prepare()
        {
            base.Prepare();
            if (MatchAction == null && NoMatchAction == null && NotSameAction == null)
                throw new InvalidOperationParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAction) + " or " + nameof(NoMatchAction) + " or " + nameof(NotSameAction));

            if (MatchAction?.Mode == MatchMode.Custom && MatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(MatchAction) + "." + nameof(MatchAction.CustomAction));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            if (NoMatchAction != null && MatchAction != null && ((NoMatchAction.Mode == MatchMode.Remove && MatchAction.Mode == MatchMode.Remove) || (NoMatchAction.Mode == MatchMode.Throw && MatchAction.Mode == MatchMode.Throw)))
                throw new InvalidOperationParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action mode: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));

            if (EqualityComparer == null)
                throw new OperationParameterNullException(this, nameof(EqualityComparer));

            Process.Context.Log(LogSeverity.Information, Process, this, "evaluating <{InputProcess}>", RightProcess.Name);

            var rightRows = RightProcess.Evaluate(Process);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                Process.Context.SetRowOwner(row, Process);

                rightRowCount++;
                var key = GetRightKey(row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, this, "fetched {RowCount} rows, lookup size is {LookupSize}", rightRowCount,
                _lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _lookup.Clear();
        }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                CounterCollection.IncrementCounter("ignored", 1);
                return;
            }

            CounterCollection.IncrementCounter("processed", 1);

            var leftKey = GetLeftKey(row);
            if (leftKey == null || !_lookup.TryGetValue(leftKey, out var match))
            {
                if (NoMatchAction != null)
                {
                    switch (NoMatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            Process.RemoveRow(row, this);
                            break;
                        case MatchMode.Throw:
                            var exception = new OperationExecutionException(Process, this, row, "no match");
                            exception.Data.Add("LeftKey", leftKey);
                            throw exception;
                        case MatchMode.Custom:
                            NoMatchAction.CustomAction.Invoke(this, row);
                            break;
                    }
                }

                return;
            }

            var isSame = EqualityComparer.Equals(row, match);
            if (isSame)
            {
                if (MatchAction != null)
                {
                    switch (MatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            Process.RemoveRow(row, this);
                            break;
                        case MatchMode.Throw:
                            var exception = new OperationExecutionException(Process, this, row, "match");
                            exception.Data.Add("LeftKey", leftKey);
                            throw exception;
                        case MatchMode.Custom:
                            MatchAction.CustomAction.Invoke(this, row, match);
                            break;
                    }
                }
            }
            else if (NotSameAction != null)
            {
                switch (NotSameAction.Mode)
                {
                    case MatchMode.Remove:
                        Process.RemoveRow(row, this);
                        break;
                    case MatchMode.Throw:
                        var exception = new OperationExecutionException(Process, this, row, "no match");
                        exception.Data.Add("LeftKey", leftKey);
                        throw exception;
                    case MatchMode.Custom:
                        NotSameAction.CustomAction.Invoke(this, row, match);
                        break;
                }
            }
        }
    }
}