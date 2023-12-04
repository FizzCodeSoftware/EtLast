namespace FizzCode.EtLast;

public class TextReaderColumn
{
    protected ITextConverter Converter { get; private set; }

    protected FailedTypeConversionAction FailedTypeConversionAction { get; private set; } = FailedTypeConversionAction.WrapError;
    protected object SpecialValueIfTypeConversionFailed { get; private set; }

    protected SourceIsNullAction SourceIsNullAction { get; private set; } = SourceIsNullAction.SetSpecialValue;
    protected object SpecialValueIfSourceIsNull { get; private set; }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public TextReaderColumn()
    {
    }

    public TextReaderColumn WithTypeConverter(ITextConverter converter)
    {
        Converter = converter;
        return this;
    }

    public TextReaderColumn FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    public TextReaderColumn ValueWhenConversionFailed(object value)
    {
        FailedTypeConversionAction = FailedTypeConversionAction.SetSpecialValue;
        SpecialValueIfTypeConversionFailed = value;
        return this;
    }

    public TextReaderColumn ValueWhenSourceIsNull(object value)
    {
        SourceIsNullAction = SourceIsNullAction.SetSpecialValue;
        SpecialValueIfSourceIsNull = value;
        return this;
    }

    public TextReaderColumn WrapErrorWhenSourceIsNull()
    {
        SourceIsNullAction = SourceIsNullAction.WrapError;
        SpecialValueIfSourceIsNull = null;
        return this;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public virtual object Process(IProcess process, TextBuilder source)
    {
        if (source.Length == 0)
        {
            return SourceIsNullAction switch
            {
                SourceIsNullAction.WrapError => new EtlRowError(process, null, "null value found"),
                SourceIsNullAction.SetSpecialValue => SpecialValueIfSourceIsNull,
                _ => throw new NotImplementedException(SourceIsNullAction.ToString() + " is not supported yet"),
            };
        }
        if (source.Length != 0 && Converter != null)
        {
            var newValue = Converter.Convert(source);
            return newValue ?? FailedTypeConversionAction switch
            {
                FailedTypeConversionAction.WrapError => new EtlRowError(process, source.GetContentAsString(), "type conversion failed (" + Converter.GetType().GetFriendlyTypeName() + ")"),
                FailedTypeConversionAction.SetSpecialValue => SpecialValueIfTypeConversionFailed,
                _ => throw new NotImplementedException(FailedTypeConversionAction.ToString() + " is not supported yet"),
            };
        }

        return source.GetContentAsString();
    }
}