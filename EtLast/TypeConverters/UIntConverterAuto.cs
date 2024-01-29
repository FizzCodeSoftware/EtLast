namespace FizzCode.EtLast;

public class UIntConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any) : UIntConverter
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

            if (uint.TryParse(stringValue, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class UIntConverterAutoFluent
{
    public static ReaderColumn AsUIntAuto(this ReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new UIntConverterAuto(formatProvider, numberStyles));
    public static TextReaderColumn AsUIntAuto(this TextReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new UIntConverterAuto(formatProvider, numberStyles));
    public static IConvertMutatorBuilder_NullStrategy ToUIntAuto(this IConvertMutatorBuilder_WithTypeConverter builder, IFormatProvider formatProvider, NumberStyles numberStyles) => builder.WithTypeConverter(new UIntConverterAuto(formatProvider, numberStyles));
}