namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class CustomExpandOperation : AbstractCrossOperation
    {
        public NoMatchMode Mode { get; set; }
        public IfDelegate If { get; set; }
        public MatchingRowSelector MatchingRowSelector { get; set; }
        public KeySelector RightKeySelector { get; set; }
        public List<(string LeftColumn, string RightColumn)> ColumnMap { get; set; }
        private readonly Dictionary<string, string> _map = new Dictionary<string, string>();
        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public CustomExpandOperation(NoMatchMode mode)
        {
            Mode = mode;
        }

        public override void Apply(IRow row)
        {
            if (If != null)
            {
                if (If.Invoke(row) == false)
                {
                    Stat.IncrementCounter("ignored", 1);
                    return;
                }
            }

            Stat.IncrementCounter("processed", 1);

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
            if (MatchingRowSelector == null) throw new InvalidOperationParameterException(this, nameof(MatchingRowSelector), MatchingRowSelector, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (RightKeySelector == null) throw new InvalidOperationParameterException(this, nameof(RightKeySelector), RightKeySelector, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (ColumnMap == null) throw new InvalidOperationParameterException(this, nameof(ColumnMap), ColumnMap, InvalidOperationParameterException.ValueCannotBeNullMessage);

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
    }
}