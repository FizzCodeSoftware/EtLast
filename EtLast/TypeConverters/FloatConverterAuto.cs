namespace FizzCode.EtLast;

public class FloatConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any) : FloatConverter
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

            if (float.TryParse(stringValue, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class FloatConverterAutoFluent
{
    public static ReaderColumn AsFloatAuto(this ReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new FloatConverterAuto(formatProvider, numberStyles));
    public static TextReaderColumn AsFloatAuto(this TextReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new FloatConverterAuto(formatProvider, numberStyles));
    public static IConvertMutatorBuilder_NullStrategy ToFloatAuto(this IConvertMutatorBuilder_WithTypeConverter builder, IFormatProvider formatProvider, NumberStyles numberStyles) => builder.WithTypeConverter(new FloatConverterAuto(formatProvider, numberStyles));
}