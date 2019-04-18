namespace FizzCode.EtLast
{
    using System;

    public class MergeDateWithTimeOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string TargetColumn { get; set; }
        public string SourceDateColumn { get; set; }
        public string SourceTimeColumn { get; set; }
        public InvalidValueAction ActionIfInvalid { get; set; } = InvalidValueAction.Throw;
        public object SpecialValueIfInvalid { get; set; }

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            var sourceDate = row[SourceDateColumn];
            var sourceTime = row[SourceTimeColumn];
            if (sourceDate != null && sourceDate is DateTime date && sourceTime != null)
            {
                if (sourceTime is DateTime dt)
                {
                    var value = new DateTime(date.Year, date.Month, date.Day, dt.Hour, dt.Minute, dt.Second);
                    row.SetValue(TargetColumn, value, this);
                    return;
                }
                else if (sourceTime is TimeSpan ts)
                {
                    var value = new DateTime(date.Year, date.Month, date.Day, ts.Hours, ts.Minutes, ts.Seconds);
                    row.SetValue(TargetColumn, value, this);
                    return;
                }
            }

            switch (ActionIfInvalid)
            {
                case InvalidValueAction.SetSpecialValue:
                    row.SetValue(TargetColumn, SpecialValueIfInvalid, this);
                    return;
                case InvalidValueAction.RemoveRow:
                    Process.RemoveRow(row, this);
                    return;
                default:
                    var exception = new OperationExecutionException(Process, this, row, "invalid value found");
                    exception.Data.Add("SourceDate", sourceDate != null ? sourceDate.ToString() + " (" + sourceDate.GetType().Name + ")" : "NULL");
                    exception.Data.Add("SourceTime", sourceTime != null ? sourceTime.ToString() + " (" + sourceTime.GetType().Name + ")" : "NULL");
                    throw exception;
            }
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(TargetColumn)) throw new InvalidOperationParameterException(this, nameof(TargetColumn), TargetColumn, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(SourceDateColumn)) throw new InvalidOperationParameterException(this, nameof(SourceDateColumn), SourceDateColumn, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(SourceTimeColumn)) throw new InvalidOperationParameterException(this, nameof(SourceTimeColumn), SourceTimeColumn, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (ActionIfInvalid != InvalidValueAction.SetSpecialValue && SpecialValueIfInvalid != null) throw new InvalidOperationParameterException(this, nameof(SpecialValueIfInvalid), SpecialValueIfInvalid, InvalidOperationParameterException.ValueCannotBeNullMessage);
        }
    }
}