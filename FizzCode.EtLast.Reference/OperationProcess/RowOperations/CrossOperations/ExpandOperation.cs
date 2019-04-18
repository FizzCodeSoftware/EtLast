namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class ExpandOperation : AbstractKeyBasedCrossOperation
    {
        public NoMatchMode Mode { get; set; }
        public IfDelegate If { get; set; }
        public List<(string LeftColumn, string RightColumn)> ColumnMap { get; set; }
        private readonly Dictionary<string, string> _map = new Dictionary<string, string>();
        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public ExpandOperation(NoMatchMode mode)
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
            if (leftKey == null || !_lookup.TryGetValue(leftKey, out IRow rightRow))
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

            foreach (var kvp in rightRow.Values)
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

            foreach (var (LeftColumn, RightColumn) in ColumnMap)
            {
                _map[RightColumn] = LeftColumn;
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

                _lookup[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rowCount, _lookup.Count);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _lookup.Clear();
            _map.Clear();
        }
    }
}