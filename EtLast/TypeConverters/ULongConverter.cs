using System.Buffers.Binary;

namespace FizzCode.EtLast;

public class ULongConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; init; }
    public bool BigEndianByteArray { get; init; } = false;

    public virtual object Convert(object source)
    {
        if (source is ulong)
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

            if (ulong.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // smaller whole numbers
        if (source is sbyte sbv && sbv >= 0)
            return System.Convert.ToUInt64(sbv);

        if (source is byte bv)
            return System.Convert.ToUInt64(bv);

        if (source is short sv && sv >= 0)
            return System.Convert.ToUInt64(sv);

        if (source is ushort usv)
            return System.Convert.ToUInt64(usv);

        if (source is int iv && iv >= 0)
            return System.Convert.ToUInt64(iv);

        if (source is uint uiv)
            return System.Convert.ToUInt64(uiv);

        // larger whole numbers
        if (source is long lv && lv >= 0)
            return System.Convert.ToUInt64(lv);

        // decimal values
        if (source is float fv && fv >= 0 && fv <= ulong.MaxValue)
            return System.Convert.ToUInt64(fv);

        if (source is double dv && dv >= 0 && dv <= ulong.MaxValue)
            return System.Convert.ToUInt64(dv);

        if (source is decimal dcv && dcv >= 0 && dcv <= ulong.MaxValue)
            return System.Convert.ToUInt64(dcv);

        if (source is bool boolv)
            return boolv ? 1UL : 0UL;

        if (source is byte[] bytes && bytes.Length == 8)
        {
            return BigEndianByteArray
                ? BinaryPrimitives.ReadUInt64BigEndian(bytes)
                : BitConverter.ToUInt64(bytes);
        }

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
public static class ULongConverterFluent
{
    public static ReaderColumn AsULong(this ReaderColumn column, bool bigEndianByteArray = false) => column.WithTypeConverter(new ULongConverter() { BigEndianByteArray = bigEndianByteArray });
    public static TextReaderColumn AsULong(this TextReaderColumn column, bool bigEndianByteArray = false) => column.WithTypeConverter(new ULongConverter() { BigEndianByteArray = bigEndianByteArray });
    public static IConvertMutatorBuilder_NullStrategy ToULong(this IConvertMutatorBuilder_WithTypeConverter builder, bool bigEndianByteArray = false) => builder.WithTypeConverter(new ULongConverter() { BigEndianByteArray = bigEndianByteArray });
}