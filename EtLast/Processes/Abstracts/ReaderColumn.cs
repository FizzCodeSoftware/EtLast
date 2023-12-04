namespace FizzCode.EtLast;

public enum FailedTypeConversionAction { SetSpecialValue, WrapError }
public enum SourceIsNullAction { SetSpecialValue, WrapError }

public class ReaderColumn
{
    protected ITypeConverter Converter { get; private set; }

    protected FailedTypeConversionAction FailedTypeConversionAction { get; private set; } = FailedTypeConversionAction.WrapError;
    protected object SpecialValueIfTypeConversionFailed { get; private set; }

    protected SourceIsNullAction SourceIsNullAction { get; private set; } = SourceIsNullAction.SetSpecialValue;
    protected object SpecialValueIfSourceIsNull { get; private set; }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public ReaderColumn()
    {
    }

    public ReaderColumn WithTypeConverter(ITypeConverter converter)
    {
        Converter = converter;
        return this;
    }

    public ReaderColumn FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    public ReaderColumn ValueWhenConversionFailed(object value)
    {
        FailedTypeConversionAction = FailedTypeConversionAction.SetSpecialValue;
        SpecialValueIfTypeConversionFailed = value;
        return this;
    }

    public ReaderColumn ValueWhenSourceIsNull(object value)
    {
        SourceIsNullAction = SourceIsNullAction.SetSpecialValue;
        SpecialValueIfSourceIsNull = value;
        return this;
    }

    public ReaderColumn WrapErrorWhenSourceIsNull()
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
            return SourceIsNullAction switch
            {
                SourceIsNullAction.WrapError => new EtlRowError(process, null, "null value found"),
                SourceIsNullAction.SetSpecialValue => SpecialValueIfSourceIsNull,
                _ => throw new NotImplementedException(SourceIsNullAction.ToString() + " is not supported yet"),
            };
        }
        if (value != null && Converter != null)
        {
            var newValue = Converter.Convert(value);
            return newValue ?? FailedTypeConversionAction switch
            {
                FailedTypeConversionAction.WrapError => new EtlRowError(process, value, "type conversion failed (" + Converter.GetType().GetFriendlyTypeName() + ")"),
                FailedTypeConversionAction.SetSpecialValue => SpecialValueIfTypeConversionFailed,
                _ => throw new NotImplementedException(FailedTypeConversionAction.ToString() + " is not supported yet"),
            };
        }

        return value;
    }
}
