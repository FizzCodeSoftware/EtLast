namespace FizzCode.EtLast;

using System;
using System.Globalization;

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
        if (source is string str)
        {
            if (Format != null)
            {
                if (DateTime.TryParseExact(str, Format, FormatProvider, DateTimeStyles, out var value))
                {
                    return value;
                }
            }
            else if (DateTime.TryParse(str, FormatProvider, DateTimeStyles, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}
