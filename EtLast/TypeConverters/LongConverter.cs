namespace FizzCode.EtLast;

public class LongConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is long)
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

            if (long.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // smaller whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToInt64(sbv);

        if (source is byte bv)
            return System.Convert.ToInt64(bv);

        if (source is short sv)
            return System.Convert.ToInt64(sv);

        if (source is ushort usv)
            return System.Convert.ToInt64(usv);

        if (source is int iv)
            return System.Convert.ToInt64(iv);

        if (source is uint uiv)
            return System.Convert.ToInt64(uiv);

        // larger whole numbers
        if (source is ulong ulv && ulv <= long.MaxValue)
            return System.Convert.ToInt64(ulv);

        // decimal values
        if (source is float fv && fv >= long.MinValue && fv <= long.MaxValue)
            return System.Convert.ToInt64(fv);

        if (source is double dv && dv >= long.MinValue && dv <= long.MaxValue)
            return System.Convert.ToInt64(dv);

        if (source is decimal dcv && dcv >= long.MinValue && dcv <= long.MaxValue)
            return System.Convert.ToInt64(dcv);

        if (source is bool boolv)
            return boolv ? 1L : 0L;

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

            if (long.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (long.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class LongConverterFluent
{
    public static ReaderColumn AsLong(this ReaderColumn column) => column.WithTypeConverter(new LongConverter());
    public static TextReaderColumn AsLong(this TextReaderColumn column) => column.WithTypeConverter(new LongConverter());
    public static IConvertMutatorBuilder_NullStrategy ToLong(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new LongConverter());
}