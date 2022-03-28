namespace FizzCode.EtLast;

public enum FailedTypeConversionAction { SetSpecialValue, WrapError }
public enum SourceIsNullAction { SetSpecialValue, WrapError }

public class ReaderColumnConfiguration : ReaderDefaultColumnConfiguration
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public ReaderColumnConfiguration(ITypeConverter converter)
        : base(converter)
    {
    }

    public ReaderColumnConfiguration FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    public new ReaderColumnConfiguration ValueWhenConversionFailed(object value)
    {
        base.ValueWhenConversionFailed(value);
        return this;
    }

    public new ReaderColumnConfiguration ValueWhenSourceIsNull(object value)
    {
        base.ValueWhenSourceIsNull(value);
        return this;
    }

    public new ReaderColumnConfiguration WrapErrorWhenSourceIsNull()
    {
        base.WrapErrorWhenSourceIsNull();
        return this;
    }
}

public class ReaderDefaultColumnConfiguration
{
    protected ITypeConverter Converter { get; }

    protected FailedTypeConversionAction FailedTypeConversionAction { get; private set; } = FailedTypeConversionAction.WrapError;
    protected object SpecialValueIfTypeConversionFailed { get; private set; }

    protected SourceIsNullAction SourceIsNullAction { get; private set; } = SourceIsNullAction.SetSpecialValue;
    protected object SpecialValueIfSourceIsNull { get; private set; }

    public ReaderDefaultColumnConfiguration(ITypeConverter converter)
    {
        Converter = converter;
    }

    public ReaderDefaultColumnConfiguration ValueWhenConversionFailed(object value)
    {
        FailedTypeConversionAction = FailedTypeConversionAction.SetSpecialValue;
        SpecialValueIfTypeConversionFailed = value;
        return this;
    }

    public ReaderDefaultColumnConfiguration ValueWhenSourceIsNull(object value)
    {
        SourceIsNullAction = SourceIsNullAction.SetSpecialValue;
        SpecialValueIfSourceIsNull = value;
        return this;
    }

    public ReaderDefaultColumnConfiguration WrapErrorWhenSourceIsNull()
    {
        SourceIsNullAction = SourceIsNullAction.WrapError;
        SpecialValueIfSourceIsNull = null;
        return this;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public virtual object Process(IProcess process, object value)
    {
        if (value == null)
        {
            switch (SourceIsNullAction)
            {
                case SourceIsNullAction.WrapError:
                    return new EtlRowError()
                    {
                        Process = process,
                        OriginalValue = null,
                        Message = string.Format(CultureInfo.InvariantCulture, "null value found"),
                    };
                case SourceIsNullAction.SetSpecialValue:
                    return SpecialValueIfSourceIsNull;
                default:
                    throw new NotImplementedException(SourceIsNullAction.ToString() + " is not supported yet");
            }
        }
        if (value != null && Converter != null)
        {
            var newValue = Converter.Convert(value);
            if (newValue != null)
                return newValue;

            switch (FailedTypeConversionAction)
            {
                case FailedTypeConversionAction.WrapError:
                    return new EtlRowError()
                    {
                        Process = process,
                        OriginalValue = value,
                        Message = string.Format(CultureInfo.InvariantCulture, "type conversion failed ({0})", Converter.GetType().GetFriendlyTypeName()),
                    };
                case FailedTypeConversionAction.SetSpecialValue:
                    return SpecialValueIfTypeConversionFailed;
                default:
                    throw new NotImplementedException(FailedTypeConversionAction.ToString() + " is not supported yet");
            }
        }

        return value;
    }
}
