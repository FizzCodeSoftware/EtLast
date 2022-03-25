namespace FizzCode.EtLast;

using System;
using System.Globalization;

public class DoubleConverterAuto : DoubleConverter
{
    public IFormatProvider FormatProvider { get; }
    public NumberStyles NumberStyles { get; }

    public DoubleConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any)
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

            if (double.TryParse(str, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}
