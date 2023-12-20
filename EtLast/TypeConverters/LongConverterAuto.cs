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

[EditorBrowsable(EditorBrowsableState.Never)]
public static class LongConverterAutoFluent
{
    public static ReaderColumn AsLongAuto(this ReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new LongConverterAuto(formatProvider, numberStyles));
    public static TextReaderColumn AsLongAuto(this TextReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new LongConverterAuto(formatProvider, numberStyles));
    public static IConvertMutatorBuilder_NullStrategy ToLongAuto(this IConvertMutatorBuilder_WithTypeConverter builder, IFormatProvider formatProvider, NumberStyles numberStyles) => builder.WithTypeConverter(new LongConverterAuto(formatProvider, numberStyles));
}