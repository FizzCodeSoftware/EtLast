namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class DeferredCustomExpandOperation : AbstractDeferredCrossOperation
    {
        public NoMatchMode Mode { get; set; }
        public MatchingRowSelector MatchingRowSelector { get; set; }
        public KeySelector RightKeySelector { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 200.
        /// </summary>
        public override int BatchSize { get; set; } = 200;

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public DeferredCustomExpandOperation(NoMatchMode mode)
        {
            Mode = mode;
        }

        public override void Prepare()
        {
            base.Prepare();

            if (MatchingRowSelector == null)
                throw new OperationParameterNullException(this, nameof(MatchingRowSelector));
            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }

        protected string GetRightKey(IProcess process, IRow row)
        {
            try
            {
                return RightKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(process, this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }

        protected override void ProcessRows(IRow[] rows)
        {
            Stat.IncrementCounter("processed", rows.Length);

            var rightProcess = RightProcessCreator.Invoke(rows);

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} getting right rows from {InputProcess}", Name, rightProcess.Name);
            _lookup.Clear();
            var rightRows = rightProcess.Evaluate(Process);
            var rowCount = 0;
            foreach (var row in rightRows)
            {
                rowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rowCount, _lookup.Count);

            foreach (var row in rows)
            {
                ProcessRow(row);
            }

            _lookup.Clear();
        }

        private void ProcessRow(IRow row)
        {
            var rightRow = MatchingRowSelector(row, _lookup);
            if (rightRow == null)
            {
                if (Mode == NoMatchMode.RemoveIfNoMatch)
                {
                    Process.RemoveRow(row, this);
                }
                else if (Mode == NoMatchMode.Throw)
                {
                    var exception = new OperationExecutionException(Process, this, row, "no right found");
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