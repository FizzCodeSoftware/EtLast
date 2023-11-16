namespace FizzCode.EtLast;

public sealed class MergeDateWithTimeMutator(IEtlContext context) : AbstractMutator(context)
{
    [ProcessParameterMustHaveValue]
    public required string TargetColumn { get; init; }

    [ProcessParameterMustHaveValue]
    public required string SourceDateColumn { get; init; }

    [ProcessParameterMustHaveValue]
    public required string SourceTimeColumn { get; init; }

    /// <summary>
    /// Default value is <see cref="InvalidValueAction.WrapError"/>
    /// </summary>
    public InvalidValueAction ActionIfInvalid { get; init; } = InvalidValueAction.WrapError;

    public object SpecialValueIfInvalid { get; init; }

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
