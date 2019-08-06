namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class DeferredExpandOperation : AbstractDeferredKeyBasedCrossOperation
    {
        public NoMatchMode Mode { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public DeferredExpandOperation(NoMatchMode mode)
        {
            Mode = mode;
        }

        public override void Prepare()
        {
            base.Prepare();
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }

        protected override void ProcessRows(IRow[] rows)
        {
            Stat.IncrementCounter("processed", rows.Length);

            var rightProcess = RightProcessCreator.Invoke(rows);

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} getting right rows from {InputProcess}", Name, rightProcess.Name);
            _lookup.Clear();
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

            foreach (var row in rows)
            {
                ProcessRow(row);
            }

            _lookup.Clear();
        }

        private void ProcessRow(IRow row)
        {
            var leftKey = GetLeftKey(Process, row);
            if (leftKey == null || !_lookup.TryGetValue(leftKey, out var rightRow))
            {
                if (Mode == NoMatchMode.RemoveIfNoMatch)
                {
                    Process.RemoveRow(row, this);
                }
                else if (Mode == NoMatchMode.Throw)
                {
                    var exception = new OperationExecutionException(Process, this, row, "no right row found");
                    exception.Data.Add("LeftKey", leftKey);
                    throw exception;
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