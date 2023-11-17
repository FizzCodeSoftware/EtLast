namespace FizzCode.EtLast;

public sealed class InPlaceConvertMutator(IEtlContext context) : AbstractSimpleChangeMutator(context)
{
    [ProcessParameterMustHaveValue]
    public required string[] Columns { get; init; }

    [ProcessParameterMustHaveValue]
    public required ITypeConverter TypeConverter { get; init; }

    // todo: all kinds of "Actions" in converters and Cross operations should use builder+subclass pattern instead of enums and secondary fields like SpecialValueIfNull...

    /// <summary>
    /// Default value is <see cref="InvalidValueAction.SetSpecialValue"/>
    /// </summary>
    public InvalidValueAction ActionIfNull { get; init; } = InvalidValueAction.SetSpecialValue;

    /// <summary>
    /// Default value is null,
    /// </summary>
    public object SpecialValueIfNull { get; init; }

    /// <summary>
    /// Default value is <see cref="InvalidValueAction.WrapError"/>
    /// </summary>
    public InvalidValueAction ActionIfInvalid { get; init; } = InvalidValueAction.WrapError;

    /// <summary>
    /// Default value is null,
    /// </summary>
    public object SpecialValueIfInvalid { get; init; }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        Changes.Clear();

        var removeRow = false;
        foreach (var column in Columns)
        {
            var source = row[column];
            if (source != null)
            {
                var value = TypeConverter.Convert(source);
                if (value != null)
                {
                    Changes.Add(new KeyValuePair<string, object>(column, value));
                    continue;
                }
            }
            else
            {
                switch (ActionIfNull)
                {
                    case InvalidValueAction.SetSpecialValue:
                        Changes.Add(new KeyValuePair<string, object>(column, SpecialValueIfNull));
                        break;
                    case InvalidValueAction.Throw:
                        throw new TypeConversionException(this, TypeConverter, row, column);
                    case InvalidValueAction.RemoveRow:
                        removeRow = true;
                        break;
                    case InvalidValueAction.WrapError:
                        Changes.Add(new KeyValuePair<string, object>(column, new EtlRowError(this, source, "null source detected by " + Name)));
                        break;
                }

                continue;
            }

            switch (ActionIfInvalid)
            {
                case InvalidValueAction.SetSpecialValue:
                    Changes.Add(new KeyValuePair<string, object>(column, SpecialValueIfInvalid));
                    break;
                case InvalidValueAction.Throw:
                    throw new TypeConversionException(this, TypeConverter, row, column);
                case InvalidValueAction.RemoveRow:
                    removeRow = true;
                    break;
                case InvalidValueAction.WrapError:
                    Changes.Add(new KeyValuePair<string, object>(column, new EtlRowError(this, source, "invalid source detected by " + Name)));
                    break;
            }
        }

        if (!removeRow)
        {
            row.MergeWith(Changes);
            yield return row;
        }
    }

    public override void ValidateParameters()
    {
        if (ActionIfInvalid != InvalidValueAction.SetSpecialValue && SpecialValueIfInvalid != null)
            throw new ProcessParameterNullException(this, nameof(SpecialValueIfInvalid));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class InPlaceConvertMutatorFluent
{
    public static IFluentSequenceMutatorBuilder ConvertValue(this IFluentSequenceMutatorBuilder builder, InPlaceConvertMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
