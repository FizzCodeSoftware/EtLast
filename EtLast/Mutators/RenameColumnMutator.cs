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

    public class RenameColumnMutator : AbstractMutator
    {
        public List<ColumnRenameConfiguration> ColumnConfiguration { get; set; }

        /// <summary>
        /// Default value is <see cref="ColumnAlreadyExistsAction.Throw"/>
        /// </summary>
        public ColumnAlreadyExistsAction ActionIfTargetValueExists { get; set; } = ColumnAlreadyExistsAction.Throw;

        public RenameColumnMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
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
                        default:
                            var exception = new ProcessExecutionException(this, row, "specified target column already exists");
                            exception.Data.Add("CurrentName", config.CurrentName);
                            exception.Data.Add("NewName", config.NewName);
                            throw exception;
                    }
                }

                var value = row[config.CurrentName];
                row.SetStagedValue(config.CurrentName, null);
                row.SetStagedValue(config.NewName, value);
            }

            if (!removeRow)
            {
                row.ApplyStaging();
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
            return builder.AddMutators(mutator);
        }
    }
}