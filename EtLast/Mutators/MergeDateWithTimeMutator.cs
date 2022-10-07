namespace FizzCode.EtLast;

public sealed class MergeDateWithTimeMutator : AbstractMutator
{
    public string TargetColumn { get; init; }
    public string SourceDateColumn { get; init; }
    public string SourceTimeColumn { get; init; }

    /// <summary>
    /// Default value is <see cref="InvalidValueAction.WrapError"/>
    /// </summary>
    public InvalidValueAction ActionIfInvalid { get; init; } = InvalidValueAction.WrapError;

    public object SpecialValueIfInvalid { get; init; }

    public MergeDateWithTimeMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var sourceDate = row[SourceDateColumn];
        var sourceTime = row[SourceTimeColumn];
        if (sourceDate is DateTime date && sourceTime != null)
        {
            if (sourceTime is DateTime dt)
            {
                row[TargetColumn] = new DateTime(date.Year, date.Month, date.Day, dt.Hour, dt.Minute, dt.Second);
                yield return row;
                yield break;
            }
            else if (sourceTime is TimeSpan ts)
            {
                row[TargetColumn] = new DateTime(date.Year, date.Month, date.Day, ts.Hours, ts.Minutes, ts.Seconds);
                yield return row;
                yield break;
            }
        }

        var removeRow = false;
        switch (ActionIfInvalid)
        {
            case InvalidValueAction.SetSpecialValue:
                row[TargetColumn] = SpecialValueIfInvalid;
                break;
            case InvalidValueAction.RemoveRow:
                removeRow = true;
                break;
            default:
                var exception = new InvalidValuesException(this, row);
                exception.Data["SourceDate"] = sourceDate != null ? sourceDate.ToString() + " (" + sourceDate.GetType().GetFriendlyTypeName() + ")" : "NULL";
                exception.Data["SourceTime"] = sourceTime != null ? sourceTime.ToString() + " (" + sourceTime.GetType().GetFriendlyTypeName() + ")" : "NULL";
                throw exception;
        }

        if (!removeRow)
            yield return row;
    }

    public override void ValidateParameters()
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

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class MergeDateWithTimeMutatorFluent
{
    public static IFluentSequenceMutatorBuilder MergeDateWithTime(this IFluentSequenceMutatorBuilder builder, MergeDateWithTimeMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
