namespace FizzCode.EtLast;

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
        if (source is string stringValue)
        {
            if (RemoveSubString != null)
            {
                foreach (var subStr in RemoveSubString)
                {
                    stringValue = stringValue.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                }
            }

            if (int.TryParse(stringValue, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}
