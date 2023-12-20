namespace FizzCode.EtLast;

public class IntConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is int)
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

            if (int.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // smaller whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToInt32(sbv);

        if (source is byte bv)
            return System.Convert.ToInt32(bv);

        if (source is short sv)
            return System.Convert.ToInt32(sv);

        if (source is ushort usv)
            return System.Convert.ToInt32(usv);

        // larger whole numbers
        if (source is uint uiv && uiv <= int.MaxValue)
            return System.Convert.ToInt32(uiv);

        if (source is long lv && lv >= int.MinValue && lv <= int.MaxValue)
            return System.Convert.ToInt32(lv);

        if (source is ulong ulv && ulv <= int.MaxValue)
            return System.Convert.ToInt32(ulv);

        // decimal values
        if (source is float fv && fv >= int.MinValue && fv <= int.MaxValue)
            return System.Convert.ToInt32(fv);

        if (source is double dv && dv >= int.MinValue && dv <= int.MaxValue)
            return System.Convert.ToInt32(dv);

        if (source is decimal dcv && dcv >= int.MinValue && dcv <= int.MaxValue)
            return System.Convert.ToInt32(dcv);

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

            if (int.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (int.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class IntConverterFluent
{
    public static ReaderColumn AsInt(this ReaderColumn column) => column.WithTypeConverter(new IntConverter());
    public static TextReaderColumn AsInt(this TextReaderColumn column) => column.WithTypeConverter(new IntConverter());
    public static IConvertMutatorBuilder_NullStrategy ToInt(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new IntConverter());
}