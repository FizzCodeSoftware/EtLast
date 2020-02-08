namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class MergeDateWithTimeMutator : AbstractMutator
    {
        public string TargetColumn { get; set; }
        public string SourceDateColumn { get; set; }
        public string SourceTimeColumn { get; set; }
        public InvalidValueAction ActionIfInvalid { get; set; } = InvalidValueAction.WrapError;
        public object SpecialValueIfInvalid { get; set; }

        public MergeDateWithTimeMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var sourceDate = row[SourceDateColumn];
            var sourceTime = row[SourceTimeColumn];
            if (sourceDate != null && sourceDate is DateTime date && sourceTime != null)
            {
                if (sourceTime is DateTime dt)
                {
                    var value = new DateTime(date.Year, date.Month, date.Day, dt.Hour, dt.Minute, dt.Second);
                    row.SetValue(TargetColumn, value, this);
                    yield return row;
                    yield break;
                }
                else if (sourceTime is TimeSpan ts)
                {
                    var value = new DateTime(date.Year, date.Month, date.Day, ts.Hours, ts.Minutes, ts.Seconds);
                    row.SetValue(TargetColumn, value, this);
                    yield return row;
                    yield break;
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

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
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