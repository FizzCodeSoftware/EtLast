namespace FizzCode.EtLast;

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
