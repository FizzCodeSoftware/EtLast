namespace FizzCode.EtLast;

public class FloatConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is float)
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

            if (float.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToSingle(sbv);

        if (source is byte bv)
            return System.Convert.ToSingle(bv);

        if (source is short sv)
            return System.Convert.ToSingle(sv);

        if (source is ushort usv)
            return System.Convert.ToSingle(usv);

        if (source is int iv)
            return System.Convert.ToSingle(iv);

        if (source is uint uiv)
            return System.Convert.ToSingle(uiv);

        if (source is long lv && lv >= float.MinValue && lv <= float.MaxValue)
            return System.Convert.ToSingle(lv);

        if (source is ulong ulv && ulv <= float.MaxValue)
            return System.Convert.ToSingle(ulv);

        // decimal values
        if (source is double dv)
            return System.Convert.ToSingle(dv);

        if (source is decimal dcv)
            return System.Convert.ToSingle(dcv);

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

            if (float.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (float.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class FloatConverterFluent
{
    public static ReaderColumn AsFloat(this ReaderColumn column) => column.WithTypeConverter(new FloatConverter());
    public static TextReaderColumn AsFloat(this TextReaderColumn column) => column.WithTypeConverter(new FloatConverter());
    public static IConvertMutatorBuilder_NullStrategy ToFloat(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new FloatConverter());
}