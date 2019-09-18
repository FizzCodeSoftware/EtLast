namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class ExpandOperation : AbstractKeyBasedCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }
        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                Stat.IncrementDebugCounter("ignored", 1);
                return;
            }

            Stat.IncrementDebugCounter("processed", 1);

            var leftKey = GetLeftKey(Process, row);
            if (leftKey == null || !_lookup.TryGetValue(leftKey, out var rightRow))
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

            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, rightRow, row);
            }
        }

        public override void Prepare()
        {
            base.Prepare();
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} getting right rows from {InputProcess}", Name, RightProcess.Name);
            _lookup.Clear();
            var rightRows = RightProcess.Evaluate(Process);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                rightRowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _lookup[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rightRowCount, _lookup.Count);
            Stat.IncrementCounter("right rows loaded", rightRowCount);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _lookup.Clear();
        }
    }
}