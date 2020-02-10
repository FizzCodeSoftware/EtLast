namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class RenameColumnsMutator : AbstractMutator
    {
        public List<ColumnRenameConfiguration> ColumnConfiguration { get; set; }
        public InvalidColumnAction ActionIfInvalid { get; set; } = InvalidColumnAction.Throw;

        public RenameColumnsMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var removeRow = false;
            foreach (var config in ColumnConfiguration)
            {
                if (row.HasValue(config.NewName))
                {
                    switch (ActionIfInvalid)
                    {
                        case InvalidColumnAction.RemoveRow:
                            removeRow = true;
                            continue;
                        case InvalidColumnAction.Skip:
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
                row.ApplyStaging(this);
                yield return row;
            }
        }

        protected override void ValidateMutator()
        {
            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}