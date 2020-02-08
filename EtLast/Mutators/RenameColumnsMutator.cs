namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class RenameColumnsMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public List<ColumnRenameConfiguration> ColumnConfiguration { get; set; }
        public InvalidColumnAction ActionIfInvalid { get; set; } = InvalidColumnAction.Throw;

        public RenameColumnsMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);

            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    yield return row;
                    continue;
                }

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
                    row.SetValue(config.CurrentName, null, this);
                    row.SetValue(config.NewName, value, this);
                }

                if (removeRow)
                {
                    Context.SetRowOwner(row, null);
                }
                else
                {
                    yield return row;
                }
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}