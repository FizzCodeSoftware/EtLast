namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class DeferredJoinOperation : AbstractDeferredKeyBasedCrossOperation
    {
        public JoinRightRowFilterDelegate RightRowFilter { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        private readonly Dictionary<string, List<IRow>> _lookup = new Dictionary<string, List<IRow>>();

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

                if (!_lookup.TryGetValue(key, out var list))
                {
                    list = new List<IRow>();
                    _lookup.Add(key, list);
                }

                list.Add(row);
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

        private void ProcessRow(IRow row)
        {
            var leftKey = GetLeftKey(Process, row);
            if (leftKey == null || !_lookup.TryGetValue(leftKey, out var rightRows) || rightRows.Count == 0)
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

            if (RightRowFilter != null)
            {
                rightRows = rightRows.Where(rightRow => RightRowFilter.Invoke(row, rightRow)).ToList();
                if (rightRows.Count == 0)
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
            }

            if (rightRows.Count > 1)
            {
                if (rightRows.Count > 2)
                {
                    var newRows = new List<IRow>();
                    for (var i = 1; i < rightRows.Count; i++)
                    {
                        var newRow = DupeRow(Process, row, rightRows[i]);
                        newRows.Add(newRow);
                    }

                    Process.AddRows(newRows, this);
                }
                else
                {
                    var newRow = DupeRow(Process, row, rightRows[1]);
                    Process.AddRow(newRow, this);
                }
            }

            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, rightRows[0], row);
            }
        }
    }
}