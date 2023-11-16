﻿namespace FizzCode.EtLast;

public class DoubleConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any) : DoubleConverter
{
    public IFormatProvider FormatProvider { get; } = formatProvider;
    public NumberStyles NumberStyles { get; } = numberStyles;

    public override object Convert(object source)
    {
        if (source is string stringValue)
        {
            if (RemoveSubString != null)
            {
                foreach (var subStr in RemoveSubString)
                {
                    stringValue = stringValue.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                }
            }

            if (double.TryParse(stringValue, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}
