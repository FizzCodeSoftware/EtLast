namespace FizzCode.EtLast;

public class ValueFormatter : IValueFormatter
{
    public static ValueFormatter Default { get; } = new ValueFormatter();

    /// <summary>
    /// Default value is "yyyy.MM.dd HH:mm:ss.fffffff"
    /// </summary>
    public string DateTimeFormat { get; init; } = "yyyy.MM.dd HH:mm:ss.fffffff";

    /// <summary>
    /// Default value is "yyyy.MM.dd HH:mm:ss.fffffff zzz"
    /// </summary>
    public string DateTimeOffsetFormat { get; init; } = "yyyy.MM.dd HH:mm:ss.fffffff zzz";

    /// <summary>
    /// Default value is "G"
    /// </summary>
    public string TimeSpanFormat { get; init; } = "G";

    /// <summary>
    /// Default value is "D"
    /// </summary>
    public string IntegerFormat { get; init; } = "G";

    /// <summary>
    /// Default value is "G"
    /// </summary>
    public string FloatingFormat { get; init; } = "G";

    /// <summary>
    /// Default value is "G"
    /// </summary>
    public string DecimalFormat { get; init; } = "G";

    /// <summary>
    /// Default value is "G"
    /// </summary>
    public string GenericFormat { get; init; } = "G";

    public string Format(object v, IFormatProvider formatProvider = null)
    {
        if (v == null)
            return null;

        if (v is string str)
            return str;

        if (v is Enum e)
            return e.GetType().Name + "." + e.ToString();

        if (v is bool b)
            return b ? "true" : "false";

        if (v is char chr)
            return chr.ToString(formatProvider);

        if (v is string[] strArr)
            return "{" + string.Join(',', strArr.Select(x => Format(x, formatProvider))) + "}";

        if (v is ISlimRow row)
            return row.ToDebugString(false);

        if (v is List<string> strList)
            return "{" + string.Join(',', strList.Select(x => Format(x, formatProvider))) + "}";

        if (v is object[] objArr)
            return "{" + string.Join(',', objArr.Select(x => Format(x, formatProvider))) + "}";

        if (v is IList genList)
        {
            var sb = new StringBuilder();
            sb.Append('{');
            foreach (var x in genList)
            {
                sb
                    .Append('\n')
                    .Append(Format(x, formatProvider));
            }
            sb.Append('}');
            return sb.ToString();
        }

        if (v is IDictionary genDict)
        {
            var sb = new StringBuilder();
            sb.Append('{');
            foreach (DictionaryEntry x in genDict)
            {
                sb
                    .Append("\n[")
                    .Append(Format(x.Key, formatProvider))
                    .Append("] = ")
                    .Append(Format(x.Value, formatProvider));
            }
            sb.Append('}');
            return sb.ToString();
        }

        if (v is int iv)
            return iv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture);

        if (v is uint uiv)
            return uiv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture);

        if (v is long lv)
            return lv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture) + "l";

        if (v is ulong ulv)
            return ulv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture) + "ul";

        if (v is double dv)
            return dv.ToString(FloatingFormat, formatProvider ?? CultureInfo.InvariantCulture) + "d";

        if (v is float fv)
            return fv.ToString(FloatingFormat, formatProvider ?? CultureInfo.InvariantCulture) + "f";

        if (v is decimal decv)
            return decv.ToString(DecimalFormat, formatProvider ?? CultureInfo.InvariantCulture) + "m";

        if (v is TimeSpan ts)
            return ts.ToString(TimeSpanFormat, formatProvider ?? CultureInfo.InvariantCulture);

        if (v is DateTime dt)
            return dt.ToString(DateTimeFormat, formatProvider ?? CultureInfo.InvariantCulture);

        if (v is DateTimeOffset dto)
            return dto.ToString(DateTimeOffsetFormat, formatProvider ?? CultureInfo.InvariantCulture);

        if (v is IFormattable fmt)
            return fmt.ToString(GenericFormat, formatProvider ?? CultureInfo.InvariantCulture);

        if (v is IProcess proc)
            return proc.Name;

        var type = v.GetType();
        if (type.BaseType == typeof(MulticastDelegate))
            return "custom delegate";

        if (type.IsGenericType)
        {
            var genType = type.GetGenericTypeDefinition();
            if (genType == typeof(KeyValuePair<,>))
            {
                var kvpKey = type.GetProperty("Key").GetValue(v, null);
                var kvpValue = type.GetProperty("Value").GetValue(v, null);

                return Format(kvpKey, formatProvider) + " = " + Format(kvpValue, formatProvider);
            }
        }

        return v.ToString();
    }
}
