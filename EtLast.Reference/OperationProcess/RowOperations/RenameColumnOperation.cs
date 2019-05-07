﻿namespace FizzCode.EtLast
{
    /// <summary>
    /// Operation doesn't treat non-existing source columns as invalid state.
    /// </summary>
    public class RenameColumnOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public ColumnRenameConfiguration ColumnConfiguration { get; set; }
        public InvalidOperationAction ActionIfInvalid { get; set; } = InvalidOperationAction.Throw;

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

            if (row.Exists(ColumnConfiguration.NewName))
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
            row.RemoveColumn(ColumnConfiguration.CurrentName, this);
            row.SetValue(ColumnConfiguration.NewName, value, this);
        }

        public override void Prepare()
        {
            if (ColumnConfiguration == null) throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}