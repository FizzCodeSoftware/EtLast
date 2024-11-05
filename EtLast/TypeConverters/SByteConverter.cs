namespace FizzCode.EtLast;

public class SByteConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }

    public virtual object Convert(object source)
    {
        if (source is sbyte)
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

            if (sbyte.TryParse(stringValue, out var value))
                return value;
        }

        if (source is byte bv && bv <= sbyte.MaxValue)
            return System.Convert.ToSByte(bv);

        // larger whole numbers
        if (source is short sv && sv >= sbyte.MinValue && sv <= sbyte.MaxValue)
            return System.Convert.ToSByte(sv);

        if (source is ushort usv && usv <= sbyte.MaxValue)
            return System.Convert.ToSByte(usv);

        if (source is int iv && iv >= sbyte.MinValue && iv <= sbyte.MaxValue)
            return System.Convert.ToSByte(iv);

        if (source is uint uiv && uiv <= sbyte.MaxValue)
            return System.Convert.ToSByte(uiv);

        if (source is long lv && lv >= sbyte.MinValue && lv <= sbyte.MaxValue)
            return System.Convert.ToSByte(lv);

        if (source is ulong ulv && ulv <= 127)
            return System.Convert.ToSByte(ulv);

        // decimal values
        if (source is float fv && fv >= sbyte.MinValue && fv <= sbyte.MaxValue)
            return System.Convert.ToSByte(fv);

        if (source is double dv && dv >= sbyte.MinValue && dv <= sbyte.MaxValue)
            return System.Convert.ToSByte(dv);

        if (source is decimal dcv && dcv >= sbyte.MinValue && dcv <= sbyte.MaxValue)
            return System.Convert.ToSByte(dcv);

        if (source is bool boolv)
            return boolv ? (sbyte)1 : (sbyte)0;

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

            if (sbyte.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (sbyte.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SByteConverterFluent
{
    public static ReaderColumn AsSByte(this ReaderColumn column) => column.WithTypeConverter(new SByteConverter());
    public static TextReaderColumn AsSByte(this TextReaderColumn column) => column.WithTypeConverter(new SByteConverter());
    public static IConvertMutatorBuilder_NullStrategy ToSByte(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new SByteConverter());
}