namespace FizzCode.EtLast;

public class DateConverterAuto : DateConverter
{
    public string Format { get; }
    public IFormatProvider FormatProvider { get; }
    public DateTimeStyles DateTimeStyles { get; }

    public DateConverterAuto(IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
    {
        FormatProvider = formatProvider;
        DateTimeStyles = dateTimeStyles;
    }

    public DateConverterAuto(string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
    {
        Format = format;
        FormatProvider = formatProvider;
        DateTimeStyles = dateTimeStyles;
    }

    public override object Convert(object source)
    {
        if (source is string str)
        {
            if (Format != null)
            {
                if (DateTime.TryParseExact(str, Format, FormatProvider, DateTimeStyles, out var value))
                {
                    return value.Date;
                }
            }
            else if (DateTime.TryParse(str, FormatProvider, DateTimeStyles, out var value))
            {
                return value.Date;
            }
        }

        return base.Convert(source);
    }
}
