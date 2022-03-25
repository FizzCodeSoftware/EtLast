namespace FizzCode.EtLast;

using System;
using System.Globalization;

public class DecimalConverterAuto : DecimalConverter
{
    public IFormatProvider FormatProvider { get; }
    public NumberStyles NumberStyles { get; }

    public DecimalConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any)
    {
        FormatProvider = formatProvider;
        NumberStyles = numberStyles;
    }

    public override object Convert(object source)
    {
        if (source is string str)
        {
            if (RemoveSubString != null)
            {
                foreach (var subStr in RemoveSubString)
                {
                    str = str.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                }
            }

            if (decimal.TryParse(str, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}
