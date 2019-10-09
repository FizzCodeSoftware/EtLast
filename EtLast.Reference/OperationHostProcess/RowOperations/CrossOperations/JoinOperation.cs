namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public delegate bool JoinRightRowFilterDelegate(IRow leftRow, IRow rightRow);

    public class JoinOperation : AbstractKeyBasedCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public JoinRightRowFilterDelegate RightRowFilter { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        private readonly Dictionary<string, List<IRow>> _lookup = new Dictionary<string, List<IRow>>();

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                Stat.IncrementDebugCounter("ignored", 1);
                return;
            }

            Stat.IncrementDebugCounter("processed", 1);

            var leftKey = GetLeftKey(row);
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
                rightRows = rightRows
                    .Where(rightRow => RightRowFilter.Invoke(row, rightRow))
                    .ToList();

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
                        MatchCustomAction?.Invoke(this, newRow, rightRows[i]);
                        newRows.Add(newRow);
                    }

                    Process.AddRows(newRows, this);
                }
                else
                {
                    var newRow = DupeRow(Process, row, rightRows[1]);
                    MatchCustomAction?.Invoke(this, newRow, rightRows[1]);
                    Process.AddRow(newRow, this);
                }
            }

            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, rightRows[0], row);
            }

            MatchCustomAction?.Invoke(this, row, rightRows[0]);
        }

        public override void Prepare()
        {
            base.Prepare();
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            Process.Context.Log(LogSeverity.Information, Process, "({OperationName}) evaluating <{InputProcess}>", Name, RightProcess.Name);
            _lookup.Clear();
            var rightRows = RightProcess.Evaluate(Process);
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

            Process.Context.Log(LogSeverity.Debug, Process, "({OperationName}) fetched {RowCount} rows, lookup size is {LookupSize}", Name, rightRowCount, _lookup.Count);
            Stat.IncrementCounter("right rows loaded", rightRowCount);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _lookup.Clear();
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
    }
}