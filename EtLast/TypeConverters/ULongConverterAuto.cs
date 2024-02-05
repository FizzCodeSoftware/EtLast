namespace FizzCode.EtLast;

public class ULongConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any) : ULongConverter
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

            if (ulong.TryParse(stringValue, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ULongConverterAutoFluent
{
    public static ReaderColumn AsULongAuto(this ReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new ULongConverterAuto(formatProvider, numberStyles));
    public static TextReaderColumn AsULongAuto(this TextReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new ULongConverterAuto(formatProvider, numberStyles));
    public static IConvertMutatorBuilder_NullStrategy ToULongAuto(this IConvertMutatorBuilder_WithTypeConverter builder, IFormatProvider formatProvider, NumberStyles numberStyles) => builder.WithTypeConverter(new ULongConverterAuto(formatProvider, numberStyles));
}