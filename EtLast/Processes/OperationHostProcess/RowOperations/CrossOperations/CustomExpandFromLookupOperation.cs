namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class CustomExpandFromLookupOperation : AbstractCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public MatchingRowFromLookupSelector MatchingRowSelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        private readonly Dictionary<string, List<IRow>> _lookup = new Dictionary<string, List<IRow>>();

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                CounterCollection.IncrementDebugCounter("ignored", 1);
                return;
            }

            CounterCollection.IncrementDebugCounter("processed", 1);

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
                            NoMatchAction.CustomAction.Invoke(this, row, null);
                            break;
                    }
                }

                return;
            }

            HandleMatch(row, rightRow);
        }

        private void HandleMatch(IRow row, IRow match)
        {
            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, match, row);
            }

            MatchCustomAction?.Invoke(this, row, match);
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

            Process.Context.Log(LogSeverity.Information, Process, this, "evaluating <{InputProcess}>", RightProcess.Name);

            _lookup.Clear();
            var rightRows = RightProcess.Evaluate(Process);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                Process.Context.SetRowOwner(row, Process);

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

            Process.Context.Log(LogSeverity.Debug, Process, this, "fetched {RowCount} rows, lookup size is {LookupSize}", rightRowCount,
                _lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);
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