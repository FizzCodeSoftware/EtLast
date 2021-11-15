namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public enum ColumnAlreadyExistsAction
    {
        Skip,
        RemoveRow,
        Throw
    }

    public sealed class RenameColumnMutator : AbstractSimpleChangeMutator
    {
        public List<ColumnRenameConfiguration> ColumnConfiguration { get; init; }

        /// <summary>
        /// Default value is <see cref="ColumnAlreadyExistsAction.Throw"/>
        /// </summary>
        public ColumnAlreadyExistsAction ActionIfTargetValueExists { get; init; } = ColumnAlreadyExistsAction.Throw;

        public RenameColumnMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            Changes.Clear();

            var removeRow = false;
            foreach (var config in ColumnConfiguration)
            {
                if (row.HasValue(config.NewName))
                {
                    switch (ActionIfTargetValueExists)
                    {
                        case ColumnAlreadyExistsAction.RemoveRow:
                            removeRow = true;
                            continue;
                        case ColumnAlreadyExistsAction.Skip:
                            continue;
                        case ColumnAlreadyExistsAction.Throw:
                            var exception = new ColumnRenameException(this, row, config.CurrentName, config.NewName);
                            throw exception;
                    }
                }

                var value = row[config.CurrentName];
                Changes.Add(new KeyValuePair<string, object>(config.CurrentName, null));
                Changes.Add(new KeyValuePair<string, object>(config.NewName, value));
            }

            if (!removeRow)
            {
                row.MergeWith(Changes);
                yield return row;
            }
        }

        protected override void ValidateMutator()
        {
            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class RenameColumnMutatorFluent
    {
        public static IFluentProcessMutatorBuilder RenameColumn(this IFluentProcessMutatorBuilder builder, RenameColumnMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}