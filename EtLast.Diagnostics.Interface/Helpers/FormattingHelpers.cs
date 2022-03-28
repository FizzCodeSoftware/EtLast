namespace FizzCode.EtLast.Diagnostics.Interface;

public static class FormattingHelpers
{
    public static string LongToString(long value)
    {
        return value.ToString("#,0", CultureInfo.InvariantCulture);
    }

    public static string TimeSpanToString(TimeSpan? value, bool detailedMilliseconds = true)
    {
        if (value == null)
            return null;

        if (value.Value.Days > 0)
        {
            return value.Value.ToString(@"d\.hh\:mm", CultureInfo.InvariantCulture);
        }
        else if (value.Value.Hours > 0)
        {
            return value.Value.ToString(@"h\:mm\:ss", CultureInfo.InvariantCulture);
        }
        else if (value.Value.Minutes > 0)
        {
            return value.Value.ToString(@"m\:ss", CultureInfo.InvariantCulture);
        }
        else
        {
            return value.Value.ToString(@"s\.f" + (detailedMilliseconds ? "ff" : ""), CultureInfo.InvariantCulture);
        }
    }

    public static string RightAlignedTimeSpanToString(TimeSpan? value, bool detailedMilliseconds = true)
    {
        if (value == null)
            return null;

        if (value.Value.Days > 0)
        {
            return value.Value.ToString(@"d\.hh\:mm\:ss\.f" + (detailedMilliseconds ? "ff" : ""), CultureInfo.InvariantCulture);
        }
        else if (value.Value.Hours > 0)
        {
            return value.Value.ToString(@"h\:mm\:ss\.f" + (detailedMilliseconds ? "ff" : ""), CultureInfo.InvariantCulture);
        }
        else if (value.Value.Minutes > 0)
        {
            return value.Value.ToString(@"m\:ss\.f" + (detailedMilliseconds ? "ff" : ""), CultureInfo.InvariantCulture);
        }
        else if (value.Value.Seconds > 0)
        {
            return value.Value.ToString(@"s\.f" + (detailedMilliseconds ? "ff" : ""), CultureInfo.InvariantCulture);
        }
        else
        {
            return value.Value.ToString(@"\.f" + (detailedMilliseconds ? "ff" : ""), CultureInfo.InvariantCulture);
        }
    }

    public static string ToDisplayValue(object value)
    {
        if (value == null)
            return "<null>";

        return value switch
        {
            bool v => v ? "true" : "false",
            char v => "\'" + v.ToString(CultureInfo.InvariantCulture) + "\'",
            string v => "\"" + v + "\"",
            string[] v => string.Join(", ", v.Select(x => "\"" + x + "\"")),
            sbyte v => v.ToString("#,0", CultureInfo.InvariantCulture),
            byte v => v.ToString("#,0", CultureInfo.InvariantCulture),
            short v => v.ToString("#,0", CultureInfo.InvariantCulture),
            ushort v => v.ToString("#,0", CultureInfo.InvariantCulture),
            int v => v.ToString("#,0", CultureInfo.InvariantCulture),
            uint v => v.ToString("#,0", CultureInfo.InvariantCulture),
            long v => LongToString(v),
            ulong v => v.ToString("#,0", CultureInfo.InvariantCulture),
            float v => v.ToString("#,0.#", CultureInfo.InvariantCulture),
            double v => v.ToString("#,0.#", CultureInfo.InvariantCulture),
            decimal v => v.ToString("#,0.#", CultureInfo.InvariantCulture),
            TimeSpan v => TimeSpanToString(v),
            DateTime v => v.ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
            DateTimeOffset v => v.ToString("yyyy.MM.dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture),
            _ => value.ToString(),
        };
    }
}
