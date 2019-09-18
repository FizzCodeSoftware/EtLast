namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class DeferredExpandOperation : AbstractDeferredKeyBasedCrossOperation
    {
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public override void Prepare()
        {
            base.Prepare();
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
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

                _lookup[key] = row;
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

            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, rightRow, row);
            }
        }
    }
}