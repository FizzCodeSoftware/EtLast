namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{" + nameof(Name) + "}")]
    public abstract class AbstractDeferredRowOperation : AbstractRowOperation, IDeferredRowOperation
    {
        public RowTestDelegate If { get; set; }
        public abstract int BatchSize { get; set; }

        /// <summary>
        /// Forces the operation to process the accumulated batch after a fixed amount of time even if the batch is not reached <see cref="BatchSize"/> yet.
        /// </summary>
        public int ForceProcessBatchAfterMilliseconds { get; set; } = 500;

        private List<IRow> _batchRows;
        private Stopwatch _lastNewRowSeenOn;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                Stat.IncrementDebugCounter("ignored", 1);
                return;
            }

            if (row.DeferState == DeferState.None)
            {
                _lastNewRowSeenOn.Restart();
                _batchRows.Add(row);
            }

            var processBatch = _batchRows.Count >= BatchSize || (_lastNewRowSeenOn.ElapsedMilliseconds >= ForceProcessBatchAfterMilliseconds && _batchRows.Count > 0);
            if (processBatch)
            {
                ProcessRows(_batchRows.ToArray());

                foreach (var batchRow in _batchRows)
                {
                    batchRow.DeferState = DeferState.DeferDone;
                }

                _batchRows.Clear();
                _lastNewRowSeenOn.Restart();
            }
            else if (row.DeferState == DeferState.None)
            {
                row.DeferState = DeferState.DeferWait; // prevent proceeding to the next operation
            }
        }

        protected abstract void ProcessRows(IRow[] rows);

        public override void Prepare()
        {
            _batchRows = new List<IRow>(BatchSize);
            _lastNewRowSeenOn = Stopwatch.StartNew();
        }

        public override void Shutdown()
        {
            _batchRows.Clear();
            _batchRows = null;
            _lastNewRowSeenOn = null;
        }
    }
}