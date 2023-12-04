namespace FizzCode.EtLast;

public sealed class ConvertInPlaceMutator : AbstractSimpleChangeMutator
{
    [ProcessParameterMustHaveValue]
    public required string[] Columns { get; init; }

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
    public static IConvertMutatorBuilder_WithTypeConverter ConvertInPlace(this IFluentSequenceMutatorBuilder builder, params string[] columns)
    {
        return new ConvertInPlaceMutatorBuilder(builder, columns);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IConvertMutatorBuilder_NullStrategy
{
    IConvertMutatorBuilder_InvalidStrategy KeepNull();
    IConvertMutatorBuilder_InvalidStrategy ChangeNullTo(object value);
    IConvertMutatorBuilder_InvalidStrategy RemoveRowIfNull();
    IConvertMutatorBuilder_InvalidStrategy ThrowIfNull();
    IConvertMutatorBuilder_InvalidStrategy WrapNull();
}

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IConvertMutatorBuilder_InvalidStrategy
{
    IFluentSequenceMutatorBuilder KeepInvalid();
    IFluentSequenceMutatorBuilder ChangeInvalidTo(object value);
    IFluentSequenceMutatorBuilder RemoveRowIfInvalid();
    IFluentSequenceMutatorBuilder ThrowIfInvalid();
    IFluentSequenceMutatorBuilder WrapInvalid();
}

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IConvertMutatorBuilder_WithTypeConverter
{
    IConvertMutatorBuilder_NullStrategy WithTypeConverter(ITypeConverter typeConverter);
}

[EditorBrowsable(EditorBrowsableState.Never)]
public class ConvertInPlaceMutatorBuilder : IConvertMutatorBuilder_NullStrategy, IConvertMutatorBuilder_InvalidStrategy, IConvertMutatorBuilder_WithTypeConverter
{
    private readonly IFluentSequenceMutatorBuilder _builder;
    private ITypeConverter _typeConverter;
    private readonly string[] _columns;
    private InvalidValueAction _actionIfNull;
    private object _valueIfNull;
    private InvalidValueAction _actionIfInvalid;
    private object _valueIfInvalid;

    internal ConvertInPlaceMutatorBuilder(IFluentSequenceMutatorBuilder builder, string[] columns)
    {
        _builder = builder;
        _columns = columns;
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
        return _builder.AddMutator(new ConvertInPlaceMutator()
        {
            Columns = _columns,
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