namespace FizzCode.EtLast;

public class DecimalConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is decimal)
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

            if (decimal.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToDecimal(sbv);

        if (source is byte bv)
            return System.Convert.ToDecimal(bv);

        if (source is short sv)
            return System.Convert.ToDecimal(sv);

        if (source is ushort usv)
            return System.Convert.ToDecimal(usv);

        if (source is int iv)
            return System.Convert.ToDecimal(iv);

        if (source is uint uiv)
            return System.Convert.ToDecimal(uiv);

        if (source is long lv && lv >= decimal.MinValue && lv <= decimal.MaxValue)
            return System.Convert.ToDecimal(lv);

        if (source is ulong ulv && ulv <= decimal.MaxValue)
            return System.Convert.ToDecimal(ulv);

        // decimal values
        if (source is double dv)
            return System.Convert.ToDecimal(dv);

        if (source is float fv)
            return System.Convert.ToDecimal(fv);

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

            if (decimal.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (decimal.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DecimalConverterFluent
{
    public static ReaderColumn AsDecimal(this ReaderColumn column) => column.WithTypeConverter(new DecimalConverter());
    public static TextReaderColumn AsDecimal(this TextReaderColumn column) => column.WithTypeConverter(new DecimalConverter());
    public static IConvertMutatorBuilder_NullStrategy ToDecimal(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new DecimalConverter());
}