namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public class DeferredExpandOperation : AbstractRowOperation, IDeferredRowOperation
    {
        public RowTestDelegate If { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Forces the operation to process the accumulated batch after a fixed amount of time even if the batch is not reached <see cref="BatchSize"/> yet.
        /// </summary>
        public int ForceProcessBatchAfterMilliseconds { get; set; } = 200;

        public MatchKeySelector LeftKeySelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }
        public Func<IRow[], IProcess> RightProcessCreator { get; set; }

        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }

        /// <summary>
        /// Default value is 100000
        /// </summary>
        public int CacheSizeLimit { get; set; } = 100000;

        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();
        private List<IRow> _batchRows;
        private HashSet<string> _batchRowKeys;
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

                var key = GetLeftKey(row);

                if (key != null && _lookup.TryGetValue(key, out var rightRow))
                {
                    Stat.IncrementCounter("served_from_cache", 1);
                    Stat.IncrementCounter("processed", 1);

                    HandleMatch(row, rightRow);
                    return;
                }

                _batchRows.Add(row);
                _batchRowKeys.Add(key);
            }

            var timeout = Process.ReadingInput
                ? ForceProcessBatchAfterMilliseconds
                : ForceProcessBatchAfterMilliseconds / 10;

            var processBatch = _batchRowKeys.Count >= BatchSize || (_lastNewRowSeenOn.ElapsedMilliseconds >= timeout && _batchRowKeys.Count > 0);
            if (processBatch)
            {
                ProcessRows();

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

        public override void Prepare()
        {
            base.Prepare();
            if (LeftKeySelector == null)
                throw new OperationParameterNullException(this, nameof(LeftKeySelector));

            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));

            if (RightProcessCreator == null)
                throw new OperationParameterNullException(this, nameof(RightProcessCreator));

            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

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

        private void ProcessRows()
        {
            Stat.IncrementCounter("processed", _batchRows.Count);

            var rightProcess = RightProcessCreator.Invoke(_batchRows.ToArray());

            Process.Context.Log(LogSeverity.Debug, Process, null, this, "evaluating <{InputProcess}> to process {RowCount} rows with {KeyCount} distinct foreign keys",
                rightProcess.Name, _batchRows.Count, _batchRowKeys.Count);

            var rightRows = rightProcess.Evaluate(Process);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                rightRowCount++;
                var key = GetRightKey(row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, null, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, _lookup.Count);

            Stat.IncrementCounter("right rows loaded", rightRowCount);

            try
            {
                foreach (var row in _batchRows)
                {
                    var key = GetLeftKey(row);
                    if (key == null || !_lookup.TryGetValue(key, out var rightRow))
                    {
                        if (NoMatchAction != null)
                        {
                            HandleNoMatch(row, key);
                        }
                    }
                    else
                    {
                        HandleMatch(row, rightRow);
                    }
                }
            }
            finally
            {
                if (_lookup.Count >= CacheSizeLimit) // caching due to the possibly N:1 nature of the operation
                {
                    _lookup.Clear();
                }
            }
        }

        private void HandleNoMatch(IRow row, string key)
        {
            switch (NoMatchAction.Mode)
            {
                case MatchMode.Remove:
                    Process.RemoveRow(row, this);
                    break;
                case MatchMode.Throw:
                    var exception = new OperationExecutionException(Process, this, row, "no match");
                    exception.Data.Add("Key", key);
                    throw exception;
                case MatchMode.Custom:
                    NoMatchAction.CustomAction.Invoke(this, row, null);
                    break;
            }
        }

        private void HandleMatch(IRow row, IRow match)
        {
            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, match, row);
            }

            MatchCustomAction?.Invoke(this, row, match);
        }

        private string GetLeftKey(IRow row)
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

        private string GetRightKey(IRow row)
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