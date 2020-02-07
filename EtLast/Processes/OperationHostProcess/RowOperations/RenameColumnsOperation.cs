namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    /// <summary>
    /// Operation doesn't treat non-existing source columns as invalid state.
    /// </summary>
    public class RenameColumnsOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public List<ColumnRenameConfiguration> ColumnConfiguration { get; set; }
        public InvalidOperationAction ActionIfInvalid { get; set; } = InvalidOperationAction.Throw;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            foreach (var config in ColumnConfiguration)
            {
                if (row.HasValue(config.NewName))
                {
                    switch (ActionIfInvalid)
                    {
                        case InvalidOperationAction.RemoveRow:
                            Process.RemoveRow(row, this);
                            return;
                        case InvalidOperationAction.Skip:
                            return;
                        default:
                            var exception = new OperationExecutionException(Process, this, row, "specified target column already exists");
                            exception.Data.Add("CurrentName", config.CurrentName);
                            exception.Data.Add("NewName", config.NewName);
                            throw exception;
                    }
                }

                var value = row[config.CurrentName];
                row.SetValue(config.CurrentName, null, this);
                row.SetValue(config.NewName, value, this);
            }
        }

        protected override void PrepareImpl()
        {
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}