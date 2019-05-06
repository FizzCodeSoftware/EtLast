namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    /// <summary>
    /// Operation doesn't treat non-existing source columns as invalid state.
    /// </summary>
    public class RenameColumnsOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public List<ColumnRenameConfiguration> ColumnConfiguration { get; set; }
        public InvalidOperationAction ActionIfInvalid { get; set; } = InvalidOperationAction.Throw;

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

            foreach (var config in ColumnConfiguration)
            {
                if (row.Exists(config.NewName))
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
                row.RemoveColumn(config.CurrentName, this);
                row.SetValue(config.NewName, value, this);
            }
        }

        public override void Prepare()
        {
            if (ColumnConfiguration == null) throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}