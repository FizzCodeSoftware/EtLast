namespace FizzCode.EtLast
{
    /// <summary>
    /// Operation doesn't treat non-existing source columns as invalid state.
    /// </summary>
    public class RenameColumnOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string CurrentName { get; set; }
        public string NewName { get; set; }
        public InvalidOperationAction ActionIfInvalid { get; set; } = InvalidOperationAction.Throw;

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            if (row.Exists(NewName))
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
                        exception.Data.Add("CurrentName", CurrentName);
                        exception.Data.Add("NewName", NewName);
                        throw exception;
                }
            }

            var value = row[CurrentName];
            row.RemoveColumn(CurrentName, this);
            row.SetValue(NewName, value, this);
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(CurrentName)) throw new InvalidOperationParameterException(this, nameof(CurrentName), CurrentName, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(NewName)) throw new InvalidOperationParameterException(this, nameof(NewName), NewName, InvalidOperationParameterException.ValueCannotBeNullMessage);
        }
    }
}