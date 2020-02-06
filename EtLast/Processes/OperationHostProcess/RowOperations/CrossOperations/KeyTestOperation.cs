namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class KeyTestOperation : AbstractKeyBasedCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }

        private readonly HashSet<string> _lookup = new HashSet<string>();

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                CounterCollection.IncrementCounter("ignored", 1);
                return;
            }

            CounterCollection.IncrementCounter("processed", 1);

            var leftKey = GetLeftKey(row);

            if (leftKey == null || !_lookup.Contains(leftKey))
            {
                if (NoMatchAction != null)
                    HandleNoMatch(row, leftKey);
            }
            else if (MatchAction != null)
            {
                HandleMatch(row, leftKey);
            }
        }

        private void HandleMatch(IRow row, string leftKey)
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
                    MatchAction.CustomAction.Invoke(this, row, row);
                    break;
            }
        }

        private void HandleNoMatch(IRow row, string leftKey)
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

            Process.Context.Log(LogSeverity.Information, Process, this, "evaluating <{InputProcess}>", RightProcess.Name);

            _lookup.Clear();
            var rightRows = RightProcess.Evaluate(Process).TakeRows(Process, true);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                rightRowCount++;
                var key = GetRightKey(row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup.Add(key);
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
    }
}