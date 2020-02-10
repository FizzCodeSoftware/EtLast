namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class CustomExpandFromLookupMutator : AbstractCrossMutator
    {
        public MatchingRowFromLookupSelector MatchingRowSelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        private Dictionary<string, List<IRow>> _lookup;

        public CustomExpandFromLookupMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void StartMutator()
        {
            _lookup = new Dictionary<string, List<IRow>>();
            var allRightRows = RightProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
            var rightRowCount = 0;
            foreach (var row in allRightRows)
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

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, _lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);
        }

        protected override void CloseMutator()
        {
            _lookup.Clear();
            _lookup = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var removeRow = false;
            var rightRow = MatchingRowSelector(row, _lookup);
            if (rightRow == null)
            {
                if (NoMatchAction != null)
                {
                    switch (NoMatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            var exception = new ProcessExecutionException(this, row, "no match");
                            throw exception;
                        case MatchMode.Custom:
                            NoMatchAction.CustomAction.Invoke(this, row);
                            break;
                    }
                }
            }
            else
            {
                ColumnCopyConfiguration.CopyManyToRowStage(rightRow, row, ColumnConfiguration);
                row.ApplyStaging(this);

                MatchCustomAction?.Invoke(this, row, rightRow);
            }

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (MatchingRowSelector == null)
                throw new ProcessParameterNullException(this, nameof(MatchingRowSelector));

            if (RightKeySelector == null)
                throw new ProcessParameterNullException(this, nameof(RightKeySelector));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
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
                var exception = new ProcessExecutionException(this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }
    }
}