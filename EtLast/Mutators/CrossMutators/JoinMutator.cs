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

        public JoinMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void StartMutator()
        {
            Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", RightProcess.Name);

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

            Context.Log(LogSeverity.Debug, this, null, "fetched {RowCount} rows, lookup size is {LookupSize}",
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
                if (rightRows.Count > 1)
                {
                    for (var i = 1; i < rightRows.Count; i++)
                    {
                        var initialValues = row.Values.ToList();
                        foreach (var config in ColumnConfiguration)
                        {
                            config.Copy(rightRows[i], initialValues);
                        }

                        var newRow = Context.CreateRow(this, initialValues);

                        MatchCustomAction?.Invoke(this, newRow, rightRows[i]);
                        yield return newRow;
                    }
                }

                foreach (var config in ColumnConfiguration)
                {
                    config.Copy(this, rightRows[0], row);
                }

                MatchCustomAction?.Invoke(this, row, rightRows[0]);
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