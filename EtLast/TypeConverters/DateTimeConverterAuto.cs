namespace FizzCode.EtLast;

public class DateTimeConverterAuto : DateTimeConverter
{
    public string Format { get; }
    public IFormatProvider FormatProvider { get; }
    public DateTimeStyles DateTimeStyles { get; }

    public DateTimeConverterAuto(IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
    {
        FormatProvider = formatProvider;
        DateTimeStyles = dateTimeStyles;
    }

    public DateTimeConverterAuto(string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
    {
        Format = format;
        FormatProvider = formatProvider;
        DateTimeStyles = dateTimeStyles;
    }

    public override object Convert(object source)
    {
        if (source is string stringValue)
        {
            if (Format != null)
            {
                if (DateTime.TryParseExact(stringValue, Format, FormatProvider, DateTimeStyles, out var value))
                {
                    return value;
                }
            }
            else if (DateTime.TryParse(stringValue, FormatProvider, DateTimeStyles, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DateTimeConverterAutoFluent
{
    public static ReaderColumn AsDateTimeAuto(this ReaderColumn column, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => column.WithTypeConverter(new DateTimeConverterAuto(formatProvider, dateTimeStyles));
    public static ReaderColumn AsDateTimeAuto(this ReaderColumn column, string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => column.WithTypeConverter(new DateTimeConverterAuto(format, formatProvider, dateTimeStyles));
    public static IConvertMutatorBuilder_NullStrategy ToDateTimeAuto(this IConvertMutatorBuilder_WithTypeConverter builder, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => builder.WithTypeConverter(new DateTimeConverterAuto(formatProvider, dateTimeStyles));
    public static IConvertMutatorBuilder_NullStrategy ToDateTimeAuto(this IConvertMutatorBuilder_WithTypeConverter builder, string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => builder.WithTypeConverter(new DateTimeConverterAuto(format, formatProvider, dateTimeStyles));
}