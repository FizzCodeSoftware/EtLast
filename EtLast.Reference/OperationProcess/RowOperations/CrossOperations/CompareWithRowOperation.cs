namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class CompareWithRowOperation : AbstractKeyBasedCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public MatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }
        public IRowEqualityComparer EqualityComparer { get; set; }

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public override void Prepare()
        {
            base.Prepare();
            if (MatchAction == null && NoMatchAction == null)
                throw new InvalidOperationParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));
            if (MatchAction?.Mode == MatchMode.Custom && MatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(MatchAction) + "." + nameof(MatchAction.CustomAction));
            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
            if (NoMatchAction != null && MatchAction != null && ((NoMatchAction.Mode == MatchMode.Remove && MatchAction.Mode == MatchMode.Remove) || (NoMatchAction.Mode == MatchMode.Throw && MatchAction.Mode == MatchMode.Throw)))
                throw new InvalidOperationParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));
            if (EqualityComparer == null)
                throw new OperationParameterNullException(this, nameof(EqualityComparer));

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} getting right rows from {InputProcess}", Name, RightProcess.Name);

            var rightRows = RightProcess.Evaluate(Process);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                rightRowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rightRowCount, _lookup.Count);
            Stat.IncrementCounter("right rows loaded", rightRowCount);
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
                Stat.IncrementDebugCounter("ignored", 1);
                return;
            }

            Stat.IncrementDebugCounter("processed", 1);

            var leftKey = GetLeftKey(Process, row);
            if (leftKey == null || !_lookup.TryGetValue(leftKey, out var rightRow))
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
                            NoMatchAction.CustomAction.Invoke(this, row, null);
                            break;
                    }
                }

                return;
            }

            var match = EqualityComparer.Compare(row, rightRow);
            if (!match)
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
                        NoMatchAction.CustomAction.Invoke(this, row, rightRow);
                        break;
                }
            }
            else
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
                        MatchAction.CustomAction.Invoke(this, row, rightRow);
                        break;
                }
            }
        }
    }
}