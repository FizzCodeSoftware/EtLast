namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public static class DefaultValueFormatter
    {
        public static string Format(object v, IFormatProvider formatProvider = null)
        {
            if (v == null)
                return null;

            if (v is string str)
                return str;

            if (v is int iv)
                return iv.ToString(null, formatProvider ?? CultureInfo.InvariantCulture);

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