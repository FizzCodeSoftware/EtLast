﻿namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public delegate bool JoinRightRowFilterDelegate(IRow leftRow, IRow rightRowRow);

    public class JoinOperation : AbstractKeyBasedCrossOperation
    {
        public NoMatchMode Mode { get; set; }
        public IfDelegate If { get; set; }
        public JoinRightRowFilterDelegate RightRowFilter { get; set; }
        public List<(string LeftColumn, string RightColumn)> ColumnMap { get; set; }
        private readonly Dictionary<string, string> _map = new Dictionary<string, string>();
        private readonly Dictionary<string, List<IRow>> _lookup = new Dictionary<string, List<IRow>>();

        public JoinOperation(NoMatchMode mode)
        {
            Mode = mode;
        }

        public override void Apply(IRow row)
        {
            if (If != null)
            {
                if (!If.Invoke(row))
                {
                    Stat.IncrementCounter("ignored", 1);
                    return;
                }
            }

            Stat.IncrementCounter("processed", 1);

            var leftKey = GetLeftKey(Process, row);
            if (leftKey == null || !_lookup.TryGetValue(leftKey, out List<IRow> rightRows) || rightRows.Count == 0)
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

            if (RightRowFilter != null)
            {
                rightRows = rightRows.Where(rightRow => RightRowFilter.Invoke(row, rightRow)).ToList();
                if (rightRows.Count == 0)
                {
                    if (Mode == NoMatchMode.RemoveIfNoMatch)
                    {
                        Process.RemoveRow(row, this);
                    }
                    else if (Mode == NoMatchMode.Throw)
                    {
                        var exception = new OperationExecutionException(Process, this, row, "no right row found after filter applied");
                        exception.Data.Add("LeftKey", leftKey);
                        throw exception;
                    }

                    return;
                }
            }

            if (rightRows.Count > 1)
            {
                if (rightRows.Count > 2)
                {
                    var newRows = new List<IRow>();
                    for (int i = 1; i < rightRows.Count; i++)
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

            foreach (var kvp in rightRows[0].Values)
            {
                if (_map.TryGetValue(kvp.Key, out string column))
                {
                    row.SetValue(column, kvp.Value, this);
                }
            }
        }

        public override void Prepare()
        {
            base.Prepare();
            if (ColumnMap == null) throw new OperationParameterNullException(this, nameof(ColumnMap));

            foreach (var (leftColumn, rightColumn) in ColumnMap)
            {
                _map[rightColumn] = leftColumn;
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} getting right rows from {InputProcess}", Name, RightProcess.Name);
            _lookup.Clear();
            var rows = RightProcess.Evaluate(Process);
            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key)) continue;

                if (!_lookup.TryGetValue(key, out List<IRow> list))
                {
                    list = new List<IRow>();
                    _lookup.Add(key, list);
                }

                list.Add(row);
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rowCount, _lookup.Count);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _lookup.Clear();
            _map.Clear();
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

            foreach (var kvp in rightRow.Values)
            {
                if (_map.TryGetValue(kvp.Key, out string column))
                {
                    newRow.SetValue(column, kvp.Value, this);
                }
            }

            return newRow;
        }
    }
}