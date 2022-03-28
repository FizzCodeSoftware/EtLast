namespace FizzCode.EtLast;

public class TimeConverterAuto : TimeConverter
{
    public string Format { get; }
    public IFormatProvider FormatProvider { get; }
    public DateTimeStyles DateTimeStyles { get; }

    public TimeConverterAuto(IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
    {
        FormatProvider = formatProvider;
        DateTimeStyles = dateTimeStyles;
    }

    public TimeConverterAuto(string format, IFormatProvider formatProvider, DateTimeStyles dateTimeStyles = DateTimeStyles.AllowWhiteSpaces)
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
                if (TimeSpan.TryParseExact(str, Format, FormatProvider, out var tsValue))
                    return tsValue;

                if (DateTime.TryParseExact(str, Format, FormatProvider, DateTimeStyles, out var dtValue))
                    return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
            }
            else
            {
                if (TimeSpan.TryParse(str, FormatProvider, out var tsValue))
                    return tsValue;

                if (DateTime.TryParse(str, FormatProvider, DateTimeStyles, out var dtValue))
                    return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
            }
        }

        return base.Convert(source);
    }
}
