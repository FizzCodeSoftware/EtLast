namespace FizzCode.EtLast;

public class TextReaderColumn : TextReaderDefaultColumn
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public TextReaderColumn()
        : base()
    {
    }

    public TextReaderColumn(ITextConverter converter)
        : base(converter)
    {
    }

    public TextReaderColumn FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    public new TextReaderColumn ValueWhenConversionFailed(object value)
    {
        base.ValueWhenConversionFailed(value);
        return this;
    }

    public new TextReaderColumn ValueWhenSourceIsNull(object value)
    {
        base.ValueWhenSourceIsNull(value);
        return this;
    }

    public new TextReaderColumn WrapErrorWhenSourceIsNull()
    {
        base.WrapErrorWhenSourceIsNull();
        return this;
    }

    public override string ToString()
    {
        return SourceColumn != null
            ? "src:" + SourceColumn
            : "";
    }
}

public class TextReaderDefaultColumn
{
    protected ITextConverter Converter { get; }

    protected FailedTypeConversionAction FailedTypeConversionAction { get; private set; } = FailedTypeConversionAction.WrapError;
    protected object SpecialValueIfTypeConversionFailed { get; private set; }

    protected SourceIsNullAction SourceIsNullAction { get; private set; } = SourceIsNullAction.SetSpecialValue;
    protected object SpecialValueIfSourceIsNull { get; private set; }

    public TextReaderDefaultColumn()
    {
    }

    public TextReaderDefaultColumn(ITextConverter converter)
    {
        Converter = converter;
    }

    public TextReaderDefaultColumn ValueWhenConversionFailed(object value)
    {
        FailedTypeConversionAction = FailedTypeConversionAction.SetSpecialValue;
        SpecialValueIfTypeConversionFailed = value;
        return this;
    }

    public TextReaderDefaultColumn ValueWhenSourceIsNull(object value)
    {
        SourceIsNullAction = SourceIsNullAction.SetSpecialValue;
        SpecialValueIfSourceIsNull = value;
        return this;
    }

    public TextReaderDefaultColumn WrapErrorWhenSourceIsNull()
    {
        SourceIsNullAction = SourceIsNullAction.WrapError;
        SpecialValueIfSourceIsNull = null;
        return this;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public virtual object Process(IProcess process, TextReaderStringBuilder value)
    {
        if (value.Length == 0)
        {
            switch (SourceIsNullAction)
            {
                case SourceIsNullAction.WrapError:
                    return new EtlRowError(process, null, "null value found");
                case SourceIsNullAction.SetSpecialValue:
                    return SpecialValueIfSourceIsNull;
                default:
                    throw new NotImplementedException(SourceIsNullAction.ToString() + " is not supported yet");
            }
        }
        if (value.Length != 0 && Converter != null)
        {
            var newValue = Converter.Convert(value);
            if (newValue != null)
                return newValue;

            switch (FailedTypeConversionAction)
            {
                case FailedTypeConversionAction.WrapError:
                    return new EtlRowError(process, value.GetContentAsString(), "type conversion failed (" + Converter.GetType().GetFriendlyTypeName() + ")");
                case FailedTypeConversionAction.SetSpecialValue:
                    return SpecialValueIfTypeConversionFailed;
                default:
                    throw new NotImplementedException(FailedTypeConversionAction.ToString() + " is not supported yet");
            }
        }

        return value.GetContentAsString();
    }
}