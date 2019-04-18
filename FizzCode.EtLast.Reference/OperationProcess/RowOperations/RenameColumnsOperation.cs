namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    /// <summary>
    /// Operation doesn't treat non-existing source columns as invalid state.
    /// </summary>
    public class RenameColumnsOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public List<(string CurrentName, string NewName)> Names { get; set; }
        public InvalidOperationAction ActionIfInvalid { get; set; } = InvalidOperationAction.Throw;

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            foreach (var (currentName, newName) in Names)
            {
                if (row.Exists(newName))
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
                            exception.Data.Add("CurrentName", currentName);
                            exception.Data.Add("NewName", newName);
                            throw exception;
                    }
                }

                var value = row[currentName];
                row.RemoveColumn(currentName, this);
                row.SetValue(newName, value, this);
            }
        }

        public override void Prepare()
        {
            if (Names == null) throw new InvalidOperationParameterException(this, nameof(Names), Names, InvalidOperationParameterException.ValueCannotBeNullMessage);
        }
    }
}