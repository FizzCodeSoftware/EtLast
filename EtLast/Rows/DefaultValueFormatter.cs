namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    public static class DefaultValueFormatter
    {
        public static string Format(object v, IFormatProvider formatProvider = null)
        {
            if (v == null)
                return null;

            if (v is string str)
                return str;

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
                return iv.ToString(null, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is long lv)
                return lv.ToString(null, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is double dv)
                return dv.ToString(null, formatProvider ?? CultureInfo.InvariantCulture);

            if (v is TimeSpan ts)
                return ts.ToString("G", formatProvider ?? CultureInfo.InvariantCulture);

            if (v is DateTime dt)
                return dt.ToString("yyyy.MM.dd HH:mm:ss.fffffff", formatProvider ?? CultureInfo.InvariantCulture);

            if (v is DateTimeOffset dto)
                return dto.ToString("yyyy.MM.dd HH:mm:ss.fffffff zzz", formatProvider ?? CultureInfo.InvariantCulture);

            if (v is IFormattable fmt)
                return fmt.ToString(null, formatProvider ?? CultureInfo.InvariantCulture);

            return v.ToString();
        }
    }
}