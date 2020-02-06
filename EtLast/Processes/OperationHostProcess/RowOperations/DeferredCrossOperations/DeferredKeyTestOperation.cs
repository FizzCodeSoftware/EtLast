namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public class DeferredKeyTestOperation : AbstractRowOperation, IDeferredRowOperation
    {
        public RowTestDelegate If { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Forces the operation to process the accumulated batch after a fixed amount of time even if the batch is not reached <see cref="BatchSize"/> yet.
        /// </summary>
        public int ForceProcessBatchAfterMilliseconds { get; set; } = 500;

        public MatchKeySelector LeftKeySelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }
        public Func<IRow[], IEvaluable> RightProcessCreator { get; set; }

        public MatchAction MatchAction { get; set; }
        public NoMatchAction NoMatchAction { get; set; }

        /// <summary>
        /// Default value is 100000
        /// </summary>
        public int CacheSizeLimit { get; set; } = 100000;

        private readonly HashSet<string> _lookup = new HashSet<string>();
        private List<IRow> _batchRows;
        private HashSet<string> _batchRowKeys;
        private Stopwatch _lastNewRowSeenOn;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                CounterCollection.IncrementCounter("ignored", 1);
                return;
            }

            if (row.DeferState == DeferState.None)
            {
                _lastNewRowSeenOn.Restart();

                var key = GetLeftKey(row);

                if (MatchAction != null && key != null && _lookup.Contains(key))
                {
                    CounterCollection.IncrementCounter("served from cache", 1, true);
                    CounterCollection.IncrementCounter("processed", 1, true);

                    HandleMatch(row, key);
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

        private void ProcessRows()
        {
            CounterCollection.IncrementCounter("processed", _batchRows.Count, true);
            CounterCollection.IncrementCounter("batches", 1, true);

            var rightProcess = RightProcessCreator.Invoke(_batchRows.ToArray());

            Process.Context.Log(LogSeverity.Debug, Process, this, "evaluating <{InputProcess}> to process {RowCount} rows with {KeyCount} distinct foreign keys", rightProcess.Name,
                _batchRows.Count, _batchRowKeys.Count);

            var rightRows = rightProcess.Evaluate(Process).TakeRows(Process, true);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                rightRowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup.Add(key);
            }

            Process.Context.Log(LogSeverity.Debug, Process, this, "fetched {RowCount} rows, lookup size is {LookupSize}", rightRowCount,
                _lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);

            try
            {
                foreach (var row in _batchRows)
                {
                    var key = GetLeftKey(row);
                    if (key == null || !_lookup.Contains(key))
                    {
                        if (NoMatchAction != null)
                        {
                            HandleNoMatch(row, key);
                        }
                    }
                    else if (MatchAction != null)
                    {
                        HandleMatch(row, key);
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
                    NoMatchAction.CustomAction.Invoke(this, row);
                    break;
            }
        }

        private void HandleMatch(IRow row, string key)
        {
            switch (MatchAction.Mode)
            {
                case MatchMode.Remove:
                    Process.RemoveRow(row, this);
                    break;
                case MatchMode.Throw:
                    var exception = new OperationExecutionException(Process, this, row, "match");
                    exception.Data.Add("Key", key);
                    throw exception;
                case MatchMode.Custom:
                    MatchAction.CustomAction.Invoke(this, row, row);
                    break;
            }
        }

        public override void Prepare()
        {
            base.Prepare();

            _batchRows = new List<IRow>(BatchSize);
            _batchRowKeys = new HashSet<string>();
            _lastNewRowSeenOn = Stopwatch.StartNew();

            if (LeftKeySelector == null)
                throw new OperationParameterNullException(this, nameof(LeftKeySelector));

            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));

            if (RightProcessCreator == null)
                throw new OperationParameterNullException(this, nameof(RightProcessCreator));

            if (MatchAction == null && NoMatchAction == null)
                throw new InvalidOperationParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));

            if (MatchAction?.Mode == MatchMode.Custom && MatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(MatchAction) + "." + nameof(MatchAction.CustomAction));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            if (NoMatchAction != null && MatchAction != null && ((NoMatchAction.Mode == MatchMode.Remove && MatchAction.Mode == MatchMode.Remove) || (NoMatchAction.Mode == MatchMode.Throw && MatchAction.Mode == MatchMode.Throw)))
                throw new InvalidOperationParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));
        }

        public override void Shutdown()
        {
            _batchRows.Clear();
            _batchRows = null;
            _batchRowKeys.Clear();
            _batchRowKeys = null;

            _lastNewRowSeenOn = null;
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

        private string GetRightKey(IProcess process, IRow row)
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
    }
}