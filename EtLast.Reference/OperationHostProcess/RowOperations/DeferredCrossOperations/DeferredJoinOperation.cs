namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public class DeferredJoinOperation : AbstractRowOperation, IDeferredRowOperation
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

        public MatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        public JoinRightRowFilterDelegate RightRowFilter { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }

        private readonly Dictionary<string, List<IRow>> _lookup = new Dictionary<string, List<IRow>>();
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
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
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

                if (!_lookup.TryGetValue(key, out var list))
                {
                    list = new List<IRow>();
                    _lookup.Add(key, list);
                }

                list.Add(row);
            }

            Process.Context.Log(LogSeverity.Debug, Process, null, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, _lookup.Count);

            Stat.IncrementCounter("right rows loaded", rightRowCount);

            try
            {
                foreach (var row in _batchRows)
                {
                    var key = GetLeftKey(row);
                    if (key == null || !_lookup.TryGetValue(key, out var matches) || matches.Count == 0)
                    {
                        if (NoMatchAction != null)
                        {
                            HandleNoMatch(row, key);
                        }

                        return;
                    }

                    if (RightRowFilter != null)
                    {
                        matches = matches.Where(rightRow => RightRowFilter.Invoke(row, rightRow)).ToList();
                        if (matches.Count == 0)
                        {
                            if (NoMatchAction != null)
                            {
                                HandleNoMatch(row, key);
                            }

                            return;
                        }
                    }

                    HandleMatch(row, matches);
                }
            }
            finally
            {
                _lookup.Clear(); // no caching due to the almost always exclusively 1:N nature of the operation
            }
        }

        private IRow DupeRow(IProcess process, IRow row, IRow rightRow)
        {
            var newRow = process.Context.CreateRow(row.ColumnCount + rightRow.ColumnCount);
            newRow.CurrentOperation = row.CurrentOperation;

            // duplicate left row
            foreach (var kvp in row.Values)
            {
                newRow.SetValue(kvp.Key, kvp.Value, this);
            }

            // join right[1..N-1] row to [1..N-1]

            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, rightRow, newRow);
            }

            return newRow;
        }

        private void HandleMatch(IRow row, List<IRow> matches)
        {
            if (matches.Count > 1)
            {
                if (matches.Count > 2)
                {
                    var newRows = new List<IRow>();
                    for (var i = 1; i < matches.Count; i++)
                    {
                        var newRow = DupeRow(Process, row, matches[i]);
                        MatchCustomAction?.Invoke(this, newRow, matches[i]);
                        newRows.Add(newRow);
                    }

                    Process.AddRows(newRows, this);
                }
                else
                {
                    var newRow = DupeRow(Process, row, matches[1]);
                    MatchCustomAction?.Invoke(this, newRow, matches[1]);
                    Process.AddRow(newRow, this);
                }
            }

            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, matches[0], row);
            }

            MatchCustomAction?.Invoke(this, row, matches[0]);
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
    }
}