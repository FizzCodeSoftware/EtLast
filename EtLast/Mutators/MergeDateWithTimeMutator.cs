namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class MergeDateWithTimeMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public string TargetColumn { get; set; }
        public string SourceDateColumn { get; set; }
        public string SourceTimeColumn { get; set; }
        public InvalidValueAction ActionIfInvalid { get; set; } = InvalidValueAction.WrapError;
        public object SpecialValueIfInvalid { get; set; }

        public MergeDateWithTimeMutator(IEtlContext context, string name, string topic)
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

                var sourceDate = row[SourceDateColumn];
                var sourceTime = row[SourceTimeColumn];
                if (sourceDate != null && sourceDate is DateTime date && sourceTime != null)
                {
                    if (sourceTime is DateTime dt)
                    {
                        var value = new DateTime(date.Year, date.Month, date.Day, dt.Hour, dt.Minute, dt.Second);
                        row.SetValue(TargetColumn, value, this);
                        yield return row;
                        continue;
                    }
                    else if (sourceTime is TimeSpan ts)
                    {
                        var value = new DateTime(date.Year, date.Month, date.Day, ts.Hours, ts.Minutes, ts.Seconds);
                        row.SetValue(TargetColumn, value, this);
                        yield return row;
                        continue;
                    }
                }

                var removeRow = false;
                switch (ActionIfInvalid)
                {
                    case InvalidValueAction.SetSpecialValue:
                        row.SetValue(TargetColumn, SpecialValueIfInvalid, this);
                        break;
                    case InvalidValueAction.RemoveRow:
                        removeRow = true;
                        break;
                    default:
                        var exception = new ProcessExecutionException(this, row, "invalid value found");
                        exception.Data.Add("SourceDate", sourceDate != null ? sourceDate.ToString() + " (" + sourceDate.GetType().GetFriendlyTypeName() + ")" : "NULL");
                        exception.Data.Add("SourceTime", sourceTime != null ? sourceTime.ToString() + " (" + sourceTime.GetType().GetFriendlyTypeName() + ")" : "NULL");
                        throw exception;
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

            if (string.IsNullOrEmpty(TargetColumn))
                throw new ProcessParameterNullException(this, nameof(TargetColumn));

            if (string.IsNullOrEmpty(SourceDateColumn))
                throw new ProcessParameterNullException(this, nameof(SourceDateColumn));

            if (string.IsNullOrEmpty(SourceTimeColumn))
                throw new ProcessParameterNullException(this, nameof(SourceTimeColumn));

            if (ActionIfInvalid != InvalidValueAction.SetSpecialValue && SpecialValueIfInvalid != null)
                throw new InvalidProcessParameterException(this, nameof(SpecialValueIfInvalid), SpecialValueIfInvalid, "value must be null if " + nameof(ActionIfInvalid) + " is not " + nameof(InvalidValueAction.SetSpecialValue));
        }
    }
}