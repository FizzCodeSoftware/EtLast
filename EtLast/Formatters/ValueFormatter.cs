namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

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
                return e.ToString();

            if (v is char chr)
                return chr.ToString(formatProvider);

            if (v is string[] strArr)
                return "[" + string.Join(',', strArr) + "]";

            if (v is ISlimRow row)
                return row.ToDebugString(false);

            if (v is List<string> strList)
                return "[" + string.Join(',', strList) + "]";

            if (v is object[] objArr)
                return "[" + string.Join(',', objArr.Select(x => Format(x, formatProvider))) + "]";

            if (v is List<object> objList)
                return "[" + string.Join(',', objList.Select(x => Format(x, formatProvider))) + "]";

            if (v is List<object[]> objArrList)
                return "[" + string.Join(',', objArrList.Select(arr => Format(arr, formatProvider))) + "]";

            if (v is Dictionary<string, object> objDict)
                return "Dictionary<string, object>:\n" + string.Join('\n', objDict.Select(x => "[\"" + x.Key + "\"] = " + Format(x.Value, formatProvider)));

            if (v is int iv)
                return iv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is uint uiv)
                return uiv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is long lv)
                return lv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is ulong ulv)
                return ulv.ToString(IntegerFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is double dv)
                return dv.ToString(FloatingFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is float fv)
                return fv.ToString(FloatingFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is decimal decv)
                return decv.ToString(DecimalFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is TimeSpan ts)
                return ts.ToString(TimeSpanFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is DateTime dt)
                return dt.ToString(DateTimeFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is DateTimeOffset dto)
                return dto.ToString(DateTimeOffsetFormat, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is IFormattable fmt)
                return fmt.ToString(GenericFormat, formatProvider ?? CultureInfo.InvariantCulture);

            return v.ToString();
        }
    }
}