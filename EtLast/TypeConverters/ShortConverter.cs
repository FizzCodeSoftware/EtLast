namespace FizzCode.EtLast;

public class ShortConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is short)
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

            if (short.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // smaller whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToInt16(sbv);

        if (source is byte bv)
            return System.Convert.ToInt16(bv);

        if (source is ushort usv)
            return System.Convert.ToInt16(usv);

        // larger whole numbers
        if (source is uint uiv && uiv <= short.MaxValue)
            return System.Convert.ToInt16(uiv);

        if (source is long lv && lv >= short.MinValue && lv <= short.MaxValue)
            return System.Convert.ToInt16(lv);

        if (source is ulong ulv && ulv <= 32767)
            return System.Convert.ToInt16(ulv);

        // decimal values
        if (source is float fv && fv >= short.MinValue && fv <= short.MaxValue)
            return System.Convert.ToInt16(fv);

        if (source is double dv && dv >= short.MinValue && dv <= short.MaxValue)
            return System.Convert.ToInt16(dv);

        if (source is decimal dcv && dcv >= short.MinValue && dcv <= short.MaxValue)
            return System.Convert.ToInt16(dcv);

        if (source is bool boolv)
            return boolv ? (short)1 : (short)0;

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

            if (short.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (short.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ShortConverterFluent
{
    public static ReaderColumn AsShort(this ReaderColumn column) => column.WithTypeConverter(new ShortConverter());
    public static TextReaderColumn AsShort(this TextReaderColumn column) => column.WithTypeConverter(new ShortConverter());
    public static IConvertMutatorBuilder_NullStrategy ToShort(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new ShortConverter());
}