namespace FizzCode.EtLast;

public class IntConverterAuto(IFormatProvider formatProvider, NumberStyles numberStyles = NumberStyles.Any) : IntConverter
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

            if (int.TryParse(stringValue, NumberStyles, FormatProvider, out var value))
            {
                return value;
            }
        }

        return base.Convert(source);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class IntConverterAutoFluent
{
    public static ReaderColumn AsIntAuto(this ReaderColumn column, IFormatProvider formatProvider, NumberStyles numberStyles) => column.WithTypeConverter(new IntConverterAuto(formatProvider, numberStyles));
    public static IConvertMutatorBuilder_NullStrategy ToIntAuto(this IConvertMutatorBuilder_WithTypeConverter builder, IFormatProvider formatProvider, NumberStyles numberStyles) => builder.WithTypeConverter(new IntConverterAuto(formatProvider, numberStyles));
}