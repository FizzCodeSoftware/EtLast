namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ExpandFromLookupMutator : AbstractCrossMutator
    {
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        public SelectRowFromLookupDelegate MatchSelector { get; set; }
        private RowLookup _lookup;

        public ExpandFromLookupMutator(ITopic topic, string name)
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
                            var exception = new ProcessExecutionException(this, row, "no match");
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
            }
            else
            {
                ColumnCopyConfiguration.CopyManyToRowStage(match, row, ColumnConfiguration);
                row.ApplyStaging();

                MatchCustomAction?.Invoke(this, row, match);
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
            return builder.AddMutators(mutator);
        }
    }
}