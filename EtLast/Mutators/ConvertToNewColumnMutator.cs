namespace FizzCode.EtLast;

public sealed class ConvertToNewColumnMutator : AbstractSimpleChangeMutator
{
    [ProcessParameterMustHaveValue]
    public required string SourceColumn { get; init; }

    [ProcessParameterMustHaveValue]
    public required string TargetColumn { get; init; }

    [ProcessParameterMustHaveValue]
    public required ITypeConverter TypeConverter { get; init; }

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

        var source = row[SourceColumn];
        if (source != null)
        {
            var value = TypeConverter.Convert(source);
            if (value != null)
            {
                Changes.Add(new KeyValuePair<string, object>(TargetColumn, value));
            }
        }
        else
        {
            switch (ActionIfNull)
            {
                case InvalidValueAction.SetSpecialValue:
                    Changes.Add(new KeyValuePair<string, object>(TargetColumn, SpecialValueIfNull));
                    break;
                case InvalidValueAction.Throw:
                    throw new TypeConversionException(this, TypeConverter, row, SourceColumn);
                case InvalidValueAction.RemoveRow:
                    removeRow = true;
                    break;
                case InvalidValueAction.WrapError:
                    Changes.Add(new KeyValuePair<string, object>(TargetColumn, new EtlRowError(this, source, "null source detected by " + Name)));
                    break;
            }
        }

        switch (ActionIfInvalid)
        {
            case InvalidValueAction.SetSpecialValue:
                Changes.Add(new KeyValuePair<string, object>(TargetColumn, SpecialValueIfInvalid));
                break;
            case InvalidValueAction.Throw:
                throw new TypeConversionException(this, TypeConverter, row, SourceColumn);
            case InvalidValueAction.RemoveRow:
                removeRow = true;
                break;
            case InvalidValueAction.WrapError:
                Changes.Add(new KeyValuePair<string, object>(TargetColumn, new EtlRowError(this, source, "invalid source detected by " + Name)));
                break;
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
public static class ConvertToNewColumnMutatorFluent
{
    public static IConvertToNewColumnMutatorBuilder_As Convert(this IFluentSequenceMutatorBuilder builder, string sourceColumn)
    {
        return new ConvertToNewColumnMutatorBuilder(builder, sourceColumn);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IConvertToNewColumnMutatorBuilder_As
{
    IConvertMutatorBuilder_WithTypeConverter Into(string targetColumn);
}

[EditorBrowsable(EditorBrowsableState.Never)]
public class ConvertToNewColumnMutatorBuilder : IConvertMutatorBuilder_NullStrategy, IConvertMutatorBuilder_InvalidStrategy, IConvertMutatorBuilder_WithTypeConverter, IConvertToNewColumnMutatorBuilder_As
{
    private readonly IFluentSequenceMutatorBuilder _builder;
    private ITypeConverter _typeConverter;
    private readonly string _sourceColumn;
    private string _targetColumn;
    private InvalidValueAction _actionIfNull;
    private object _valueIfNull;
    private InvalidValueAction _actionIfInvalid;
    private object _valueIfInvalid;

    internal ConvertToNewColumnMutatorBuilder(IFluentSequenceMutatorBuilder builder, string sourceColumn)
    {
        _builder = builder;
        _sourceColumn = sourceColumn;
    }

    public IConvertMutatorBuilder_WithTypeConverter Into(string targetColumn)
    {
        _targetColumn = targetColumn;
        return this;
    }

    public IConvertMutatorBuilder_InvalidStrategy KeepNull() => this;

    public IConvertMutatorBuilder_InvalidStrategy ChangeNullTo(object value)
    {
        _actionIfNull = InvalidValueAction.SetSpecialValue;
        _valueIfNull = value;
        return this;
    }

    public IConvertMutatorBuilder_InvalidStrategy RemoveRowIfNull()
    {
        _actionIfNull = InvalidValueAction.RemoveRow;
        return this;
    }

    public IConvertMutatorBuilder_InvalidStrategy ThrowIfNull()
    {
        _actionIfNull = InvalidValueAction.Throw;
        return this;
    }

    public IConvertMutatorBuilder_InvalidStrategy WrapNull()
    {
        _actionIfNull = InvalidValueAction.WrapError;
        return this;
    }

    public IFluentSequenceMutatorBuilder KeepInvalid() => Finish();

    public IFluentSequenceMutatorBuilder ChangeInvalidTo(object value)
    {
        _actionIfInvalid = InvalidValueAction.SetSpecialValue;
        _valueIfInvalid = value;
        return Finish();
    }

    public IFluentSequenceMutatorBuilder RemoveRowIfInvalid()
    {
        _actionIfInvalid = InvalidValueAction.RemoveRow;
        return Finish();
    }

    public IFluentSequenceMutatorBuilder ThrowIfInvalid()
    {
        _actionIfInvalid = InvalidValueAction.Throw;
        return Finish();
    }

    public IFluentSequenceMutatorBuilder WrapInvalid()
    {
        _actionIfInvalid = InvalidValueAction.WrapError;
        return Finish();
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public IConvertMutatorBuilder_NullStrategy WithTypeConverter(ITypeConverter typeConverter)
    {
        _typeConverter = typeConverter;
        return this;
    }

    private IFluentSequenceMutatorBuilder Finish()
    {
        return _builder.AddMutator(new ConvertToNewColumnMutator()
        {
            SourceColumn = _sourceColumn,
            TargetColumn = _targetColumn,
            TypeConverter = _typeConverter,
            ActionIfNull = _actionIfNull,
            SpecialValueIfNull = _valueIfNull,
            ActionIfInvalid = _actionIfInvalid,
            SpecialValueIfInvalid = _valueIfInvalid,
        });
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public override bool Equals(object obj)
    {
        return base.Equals(obj);
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public override string ToString()
    {
        return base.ToString();
    }
}