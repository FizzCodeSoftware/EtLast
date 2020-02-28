namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public delegate bool JoinRightRowFilterDelegate(IRow leftRow, IRow rightRow);

    public class JoinMutator : AbstractKeyBasedCrossMutator
    {
        public JoinRightRowFilterDelegate RightRowFilter { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        private Dictionary<string, List<IRow>> _lookup;

        public JoinMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lookup = new Dictionary<string, List<IRow>>();
            var allRightRows = RightProcess.Evaluate(this).TakeRowsAndReleaseOwnership();
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
            var leftKey = GetLeftKey(row);
            List<IRow> rightRows = null;
            if (leftKey != null)
                _lookup.TryGetValue(leftKey, out rightRows);

            if (rightRows != null && RightRowFilter != null)
            {
                rightRows = rightRows
                    .Where(rightRow => RightRowFilter.Invoke(row, rightRow))
                    .ToList();
            }

            var removeRow = false;
            if (rightRows?.Count > 0)
            {
                removeRow = true;
                foreach (var rightRow in rightRows)
                {
                    var initialValues = new Dictionary<string, object>(row.Values);
                    ColumnCopyConfiguration.CopyMany(rightRow, initialValues, ColumnConfiguration);

                    var newRow = Context.CreateRow(this, initialValues);

                    MatchCustomAction?.Invoke(this, newRow, rightRow);
                    yield return newRow;
                }
            }
            else if (NoMatchAction != null)
            {
                switch (NoMatchAction.Mode)
                {
                    case MatchMode.Remove:
                        removeRow = true;
                        break;
                    case MatchMode.Throw:
                        var exception = new ProcessExecutionException(this, row, "no match");
                        exception.Data.Add("LeftKey", leftKey);
                        throw exception;
                    case MatchMode.Custom:
                        NoMatchAction.CustomAction.Invoke(this, row);
                        break;
                }
            }

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
        }
    }
}