namespace FizzCode.EtLast;

using System;
using System.Globalization;

public class IntConverterAuto : IntConverter
{
    public IFormatProvider FormatProvider { get; }
    public NumberStyles NumberStyles { get; }

    public IntConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any)
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

            if (int.TryParse(str, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}
