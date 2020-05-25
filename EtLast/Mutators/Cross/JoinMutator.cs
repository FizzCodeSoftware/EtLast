namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class JoinMutator : AbstractCrossMutator
    {
        public RowKeyGenerator RowKeyGenerator { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        public Func<IReadOnlySlimRow, bool> MatchFilter { get; set; }

        /// <summary>
        /// Default null. If value is set, and <see cref="TooManyMatchAction"/> is null,
        /// then the excess rows will be removed, otherwise the action will be invoked.
        /// </summary>
        public int? MatchCountLimit { get; set; }

        /// <summary>
        /// Executed if the match count for a row exceeds <see cref="MatchCountLimit"/>.
        /// </summary>
        public TooManyMatchAction TooManyMatchAction { get; set; }

        private RowLookup _lookup;

        public JoinMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lookup = LookupBuilder.Build(this);
        }

        protected override void CloseMutator()
        {
            _lookup.Clear();
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var key = GenerateRowKey(row);
            var removeRow = false;
            var matches = _lookup.GetManyByKey(key, MatchFilter);
            if (MatchCountLimit != null && matches?.Count > MatchCountLimit.Value)
            {
                if (TooManyMatchAction != null)
                {
                    switch (TooManyMatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            var exception = new ProcessExecutionException(this, row, "too many match");
                            exception.Data.Add("Key", key);
                            throw exception;
                        case MatchMode.Custom:
                            TooManyMatchAction.InvokeCustomAction(this, row, matches);
                            break;
                        case MatchMode.CustomThenRemove:
                            removeRow = true;
                            TooManyMatchAction.InvokeCustomAction(this, row, matches);
                            break;
                    }
                }
                else
                {
                    matches.RemoveRange(MatchCountLimit.Value, matches.Count - MatchCountLimit.Value);
                }
            }

            if (!removeRow && matches?.Count > 0)
            {
                removeRow = true;
                foreach (var match in matches)
                {
                    var initialValues = new Dictionary<string, object>(row.Values);
                    ColumnCopyConfiguration.CopyMany(match, initialValues, ColumnConfiguration);

                    var newRow = Context.CreateRow(this, initialValues);

                    InvokeCustomMatchAction(row, newRow, match);

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
                        exception.Data.Add("Key", key);
                        throw exception;
                    case MatchMode.Custom:
                        NoMatchAction.InvokeCustomAction(this, row);
                        break;
                    case MatchMode.CustomThenRemove:
                        removeRow = true;
                        NoMatchAction.InvokeCustomAction(this, row);
                        break;
                }
            }

            if (!removeRow)
                yield return row;
        }

        private void InvokeCustomMatchAction(IReadOnlySlimRow row, IRow newRow, IReadOnlySlimRow match)
        {
            try
            {
                MatchCustomAction?.Invoke(this, newRow, match);
            }
            catch (Exception ex) when (!(ex is EtlException))
            {
                var exception = new ProcessExecutionException(this, row, "error during the execution of a " + nameof(MatchCustomAction) + " delegate", ex);
                exception.Data.Add("Row-New", newRow.ToDebugString());
                exception.Data.Add("Row-Match", match.ToDebugString());
                throw exception;
            }
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (RowKeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(RowKeyGenerator));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
        }

        private string GenerateRowKey(IReadOnlyRow row)
        {
            try
            {
                return RowKeyGenerator(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new ProcessExecutionException(this, row, nameof(RowKeyGenerator) + " failed");
                throw exception;
            }
        }
    }
}