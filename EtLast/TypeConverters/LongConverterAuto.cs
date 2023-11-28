namespace FizzCode.EtLast;

public class LongConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any) : LongConverter
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

            if (long.TryParse(stringValue, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class LongConverterAutoFluent
{
    public static ReaderColumn AsLongAuto(this ReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new LongConverterAuto(formatProvider, numberStyles));
}