namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class DeferredValidateForeignKeyOperation : AbstractDeferredKeyBasedCrossOperation
    {
        public MatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        private readonly HashSet<string> _lookup = new HashSet<string>();

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
        }

        protected override void ProcessRows(IRow[] rows)
        {
            Stat.IncrementCounter("processed", rows.Length);

            var rightProcess = RightProcessCreator.Invoke(rows);

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} getting right rows from {InputProcess}", Name, rightProcess.Name);

            var rightRows = rightProcess.Evaluate(Process);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                rightRowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup.Add(key);
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rightRowCount, _lookup.Count);
            Stat.IncrementCounter("right rows loaded", rightRowCount);

            try
            {
                foreach (var row in rows)
                {
                    ProcessRow(row);
                }
            }
            finally
            {
                _lookup.Clear();
            }
        }

        private void ProcessRow(IRow row)
        {
            var leftKey = GetLeftKey(Process, row);

            if (leftKey == null || !_lookup.Contains(leftKey))
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
            }
            else if (MatchAction != null)
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
                        MatchAction.CustomAction.Invoke(this, row);
                        break;
                }
            }
        }
    }
}