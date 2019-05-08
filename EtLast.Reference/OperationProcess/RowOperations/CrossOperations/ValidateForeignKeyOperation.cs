namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class ValidateForeignKeyOperation : AbstractKeyBasedCrossOperation
    {
        public ForeignKeyValidationMode Mode { get; set; }
        public IfDelegate If { get; set; }
        private readonly HashSet<string> _lookup = new HashSet<string>();

        public ValidateForeignKeyOperation(ForeignKeyValidationMode mode)
        {
            Mode = mode;
        }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                Stat.IncrementCounter("ignored", 1);
                return;
            }

            Stat.IncrementCounter("processed", 1);

            var leftKey = GetLeftKey(Process, row);

            if (Mode == ForeignKeyValidationMode.KeepIfExists)
            {
                if (leftKey != null && _lookup.Contains(leftKey))
                {
                    return;
                }

                Process.RemoveRow(row, this);
            }
            else
            {
                if (leftKey != null && _lookup.Contains(leftKey))
                {
                    Process.RemoveRow(row, this);
                }
            }
        }

        public override void Prepare()
        {
            base.Prepare();

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} getting right rows from {InputProcess}", Name, RightProcess.Name);
            _lookup.Clear();
            var rows = RightProcess.Evaluate(Process);
            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup.Add(key);
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rowCount, _lookup.Count);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _lookup.Clear();
        }
    }
}