namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public sealed class ExpandFromLookupMutator : AbstractCrossMutator
    {
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; init; }
        public NoMatchAction NoMatchAction { get; init; }
        public MatchActionDelegate MatchCustomAction { get; init; }
        public SelectRowFromLookupDelegate MatchSelector { get; init; }

        private RowLookup _lookup;
        private List<KeyValuePair<string, object>> _changes;

        public ExpandFromLookupMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lookup = LookupBuilder.Build(this);
            _changes = new List<KeyValuePair<string, object>>();
        }

        protected override void CloseMutator()
        {
            _lookup.Clear();
            _lookup = null;

            _changes.Clear();
            _changes = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var removeRow = false;
            var match = MatchSelector(row, _lookup);
            if (match == null)
            {
                if (NoMatchAction != null)
                {
                    switch (NoMatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            throw new NoMatchException(this, row);
                        case MatchMode.Custom:
                            NoMatchAction.InvokeCustomAction(row);
                            break;
                        case MatchMode.CustomThenRemove:
                            removeRow = true;
                            NoMatchAction.InvokeCustomAction(row);
                            break;
                    }
                }
            }
            else
            {
                _changes.Clear();
                foreach (var config in ColumnConfiguration)
                {
                    _changes.Add(new KeyValuePair<string, object>(config.ToColumn, match[config.FromColumn]));
                }
                row.MergeWith(_changes);

                MatchCustomAction?.Invoke(row, match);
            }

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (MatchSelector == null)
                throw new ProcessParameterNullException(this, nameof(MatchSelector));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ExpandFromLookupMutatorFluent
    {
        /// <summary>
        /// Copy columns to input rows from existing rows using a custom selector.
        /// - <see cref="ExpandFromLookupMutator.MatchSelector"/> can select 0 or 1 row from a single <see cref="RowLookup"/> for each row
        /// </summary>
        public static IFluentProcessMutatorBuilder ExpandFromLookup(this IFluentProcessMutatorBuilder builder, ExpandFromLookupMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}