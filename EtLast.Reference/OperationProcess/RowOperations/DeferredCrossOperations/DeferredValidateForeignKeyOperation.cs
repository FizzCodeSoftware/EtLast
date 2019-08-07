namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class DeferredValidateForeignKeyOperation : AbstractDeferredKeyBasedCrossOperation
    {
        public ForeignKeyValidationMode Mode { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        private readonly HashSet<string> _lookup = new HashSet<string>();

        public DeferredValidateForeignKeyOperation(ForeignKeyValidationMode mode)
        {
            Mode = mode;
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

            if (Mode == ForeignKeyValidationMode.KeepIfExists)
            {
                if (leftKey == null || !_lookup.Contains(leftKey))
                {
                    Process.RemoveRow(row, this);
                }
            }
            else if (leftKey != null && _lookup.Contains(leftKey))
            {
                Process.RemoveRow(row, this);
            }
        }
    }
}