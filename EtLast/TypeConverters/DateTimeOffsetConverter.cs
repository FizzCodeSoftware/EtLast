namespace FizzCode.EtLast;

public class DateTimeOffsetConverter : ITypeConverter, ITextConverter
{
    public virtual object Convert(object source)
    {
        if (source is DateTimeOffset)
            return source;

        if (source is string stringValue)
        {
            if (DateTimeOffset.TryParse(stringValue, out var value))
                return value;
        }

        return null;
    }

    public object Convert(TextBuilder source)
    {
        var span = source.GetContentAsSpan();
        if (DateTime.TryParse(span, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var value))
            return value;

        return null;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DateTimeOffsetConverterFluent
{
    public static ReaderColumn AsDateTimeOffset(this ReaderColumn column) => column.WithTypeConverter(new DateTimeOffsetConverter());
    public static TextReaderColumn AsDateTimeOffset(this TextReaderColumn column) => column.WithTypeConverter(new DateTimeOffsetConverter());
    public static IConvertMutatorBuilder_NullStrategy ToDateTimeOffset(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new DateTimeOffsetConverter());
}