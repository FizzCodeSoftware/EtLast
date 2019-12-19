namespace FizzCode.EtLast
{
    /// <summary>
    /// Operation doesn't treat non-existing source columns as invalid state.
    /// </summary>
    public class RenameColumnOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public ColumnRenameConfiguration ColumnConfiguration { get; set; }
        public InvalidOperationAction ActionIfInvalid { get; set; } = InvalidOperationAction.Throw;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (row.HasValue(ColumnConfiguration.NewName))
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
                        exception.Data.Add("CurrentName", ColumnConfiguration.CurrentName);
                        exception.Data.Add("NewName", ColumnConfiguration.NewName);
                        throw exception;
                }
            }

            var value = row[ColumnConfiguration.CurrentName];
            row.SetValue(ColumnConfiguration.CurrentName, null, this);
            row.SetValue(ColumnConfiguration.NewName, value, this);
        }

        public override void Prepare()
        {
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}