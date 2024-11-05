namespace FizzCode.EtLast;

public class UShortConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is ushort)
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

            if (ushort.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // smaller whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToUInt16(sbv);

        if (source is byte bv)
            return System.Convert.ToUInt16(bv);

        if (source is short sv)
            return System.Convert.ToUInt16(sv);

        // larger whole numbers
        if (source is int iv && iv >= 0)
            return System.Convert.ToUInt16(iv);

        if (source is long lv && lv >= 0 && lv <= ushort.MaxValue)
            return System.Convert.ToUInt16(lv);

        if (source is ulong ulv && ulv <= uint.MaxValue)
            return System.Convert.ToUInt16(ulv);

        // decimal values
        if (source is float fv && fv >= 0 && fv <= ushort.MaxValue)
            return System.Convert.ToUInt16(fv);

        if (source is double dv && dv >= 0 && dv <= ushort.MaxValue)
            return System.Convert.ToUInt16(dv);

        if (source is decimal dcv && dcv >= 0 && dcv <= ushort.MaxValue)
            return System.Convert.ToUInt16(dcv);

        if (source is bool boolv)
            return boolv ? (ushort)1 : (ushort)0;

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

            if (ushort.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (ushort.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class UShortConverterFluent
{
    public static ReaderColumn AsUShort(this ReaderColumn column) => column.WithTypeConverter(new UShortConverter());
    public static TextReaderColumn AsUIntUShort(this TextReaderColumn column) => column.WithTypeConverter(new UShortConverter());
    public static IConvertMutatorBuilder_NullStrategy ToUShort(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new UShortConverter());
}