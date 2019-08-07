namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class CustomExpandOperation : AbstractCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public MatchingRowSelector MatchingRowSelector { get; set; }
        public KeySelector RightKeySelector { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }
        private readonly Dictionary<string, IRow> _lookup = new Dictionary<string, IRow>();

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                Stat.IncrementCounter("ignored", 1);
                return;
            }

            Stat.IncrementCounter("processed", 1);

            var rightRow = MatchingRowSelector(row, _lookup);
            if (rightRow == null)
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
                            throw exception;
                        case MatchMode.Custom:
                            NoMatchAction.CustomAction.Invoke(this, row);
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

            if (MatchingRowSelector == null)
                throw new OperationParameterNullException(this, nameof(MatchingRowSelector));
            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

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

                _lookup[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, "{OperationName} fetched {RowCount} rows, lookup size is {LookupSize}", Name, rowCount, _lookup.Count);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _lookup.Clear();
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