namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public abstract class AbstractDeferredKeyBasedCrossOperation : AbstractRowOperation, IDeferredRowOperation
    {
        public RowTestDelegate If { get; set; }
        public abstract int BatchSize { get; set; }

        /// <summary>
        /// Forces the operation to process the accumulated batch after a fixed amount of time even if the batch is not reached <see cref="BatchSize"/> yet.
        /// </summary>
        public int ForceProcessBatchAfterMilliseconds { get; set; } = 200;

        private List<IRow> _batchRows;
        private HashSet<string> _batchRowKeys;
        private Stopwatch _lastNewRowSeenOn;

        public MatchKeySelector LeftKeySelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }
        public Func<IRow[], IProcess> RightProcessCreator { get; set; }

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

                var key = GetLeftKey(row);
                _batchRows.Add(row);
                _batchRowKeys.Add(key);
            }

            var timeout = Process.ReadingInput
                ? ForceProcessBatchAfterMilliseconds
                : ForceProcessBatchAfterMilliseconds / 10;

            var processBatch = _batchRowKeys.Count >= BatchSize || (_lastNewRowSeenOn.ElapsedMilliseconds >= timeout && _batchRowKeys.Count > 0);
            if (processBatch)
            {
                if (_batchRowKeys.Count >= BatchSize)
                {
                    Process.Context.Log(LogSeverity.Information, Process, null, this, "processing {RowCount} batch rows with {KeyCount} distinct foreign keys",
                       Name, _batchRows.Count, _batchRowKeys.Count);
                }
                else
                {
                    Process.Context.Log(LogSeverity.Information, Process, null, this, "processing {RowCount} batch rows with {KeyCount} distinct foreign keys ({Elapsed} of {Timeout} msec)",
                       Name, _batchRows.Count, _batchRowKeys.Count, _lastNewRowSeenOn.ElapsedMilliseconds, timeout);
                }

                ProcessRows(_batchRows.ToArray());

                foreach (var batchRow in _batchRows)
                {
                    batchRow.DeferState = DeferState.DeferDone;
                }

                _batchRows.Clear();
                _batchRowKeys.Clear();
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
            base.Prepare();
            if (LeftKeySelector == null)
                throw new OperationParameterNullException(this, nameof(LeftKeySelector));
            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));
            if (RightProcessCreator == null)
                throw new OperationParameterNullException(this, nameof(RightProcessCreator));

            _batchRows = new List<IRow>(BatchSize);
            _batchRowKeys = new HashSet<string>();
            _lastNewRowSeenOn = Stopwatch.StartNew();
        }

        public override void Shutdown()
        {
            _batchRows.Clear();
            _batchRows = null;
            _batchRowKeys.Clear();
            _batchRowKeys = null;

            _lastNewRowSeenOn = null;

            base.Shutdown();
        }

        protected string GetLeftKey(IRow row)
        {
            try
            {
                return LeftKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(Process, this, row, nameof(LeftKeySelector) + " failed");
                throw exception;
            }
        }

        protected string GetRightKey(IRow row)
        {
            try
            {
                return RightKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(Process, this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }
    }
}