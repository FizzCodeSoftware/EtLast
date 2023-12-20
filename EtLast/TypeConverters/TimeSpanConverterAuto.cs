namespace FizzCode.EtLast;

public class TimeSpanConverterAuto : TimeSpanConverter
{
    public string Format { get; }
    public IFormatProvider FormatProvider { get; }
    public DateTimeStyles DateTimeStyles { get; }

    public TimeSpanConverterAuto(IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
    {
        FormatProvider = formatProvider;
        DateTimeStyles = dateTimeStyles;
    }

    public TimeSpanConverterAuto(string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
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
                if (TimeSpan.TryParseExact(stringValue, Format, FormatProvider, out var tsValue))
                    return tsValue;

                if (DateTime.TryParseExact(stringValue, Format, FormatProvider, DateTimeStyles, out var dtValue))
                    return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
            }
            else
            {
                if (TimeSpan.TryParse(stringValue, FormatProvider, out var tsValue))
                    return tsValue;

                if (DateTime.TryParse(stringValue, FormatProvider, DateTimeStyles, out var dtValue))
                    return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
            }
        }

        return base.Convert(source);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class TimeSpanConverterAutoFluent
{
    public static ReaderColumn AsTimeSpanAuto(this ReaderColumn column, IFormatProvider formatProvider = null, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => column.WithTypeConverter(new TimeSpanConverterAuto(formatProvider, dateTimeStyles));
    public static ReaderColumn AsTimeSpanAuto(this ReaderColumn column, string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => column.WithTypeConverter(new TimeSpanConverterAuto(format, formatProvider, dateTimeStyles));
    public static IConvertMutatorBuilder_NullStrategy ToTimeSpanAuto(this IConvertMutatorBuilder_WithTypeConverter builder, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => builder.WithTypeConverter(new TimeSpanConverterAuto(formatProvider, dateTimeStyles));
    public static IConvertMutatorBuilder_NullStrategy ToTimeSpanAuto(this IConvertMutatorBuilder_WithTypeConverter builder, string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces) => builder.WithTypeConverter(new TimeSpanConverterAuto(format, formatProvider, dateTimeStyles));
}