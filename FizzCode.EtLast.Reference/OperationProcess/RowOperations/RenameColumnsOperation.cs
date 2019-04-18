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
            var result = If?.Invoke(row) != false;
            if (!result) return;

            foreach (var (CurrentName, NewName) in Names)
            {
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
        }

        public override void Prepare()
        {
            if (Names == null) throw new OperationParameterNullException(this, nameof(Names));
        }
    }
}