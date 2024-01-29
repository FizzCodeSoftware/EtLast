namespace FizzCode.EtLast;

public class UIntConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is uint)
            return source;

        if (source is string stringValue)
        {
            if (RemoveSubString != null)
            {
                foreach (var subStr in RemoveSubString)
                {
                    stringValue = stringValue.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                }
            }

            if (uint.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // smaller whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToUInt32(sbv);

        if (source is byte bv)
            return System.Convert.ToUInt32(bv);

        if (source is short sv)
            return System.Convert.ToUInt32(sv);

        if (source is ushort usv)
            return System.Convert.ToUInt32(usv);

        // larger whole numbers
        if (source is int iv && iv >= 0)
            return System.Convert.ToUInt32(iv);

        if (source is long lv && lv >= 0 && lv <= uint.MaxValue)
            return System.Convert.ToUInt32(lv);

        if (source is ulong ulv && ulv <= uint.MaxValue)
            return System.Convert.ToUInt32(ulv);

        // decimal values
        if (source is float fv && fv >= 0 && fv <= uint.MaxValue)
            return System.Convert.ToUInt32(fv);

        if (source is double dv && dv >= 0 && dv <= uint.MaxValue)
            return System.Convert.ToUInt32(dv);

        if (source is decimal dcv && dcv >= 0 && dcv <= uint.MaxValue)
            return System.Convert.ToUInt32(dcv);

        if (source is bool boolv)
            return boolv ? 1 : 0;

        return null;
    }

    public object Convert(TextBuilder source)
    {
        if (RemoveSubString != null)
        {
            var stringValue = source.GetContentAsString();
            foreach (var subStr in RemoveSubString)
            {
                stringValue = stringValue.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
            }

            if (uint.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (uint.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class UIntConverterFluent
{
    public static ReaderColumn AsUInt(this ReaderColumn column) => column.WithTypeConverter(new UIntConverter());
    public static TextReaderColumn AsUInt(this TextReaderColumn column) => column.WithTypeConverter(new UIntConverter());
    public static IConvertMutatorBuilder_NullStrategy ToUInt(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new UIntConverter());
}