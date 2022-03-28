namespace FizzCode.EtLast.Diagnostics.Interface;

public static class Extensions
{
    public static string FormatToString(this int number, string format = "#,0")
    {
        return number.ToString(format, CultureInfo.InvariantCulture);
    }

    public static string FormatToStringNoZero(this int number, string format = "#,0")
    {
        if (number == 0)
            return "";

        return FormatToString(number, format);
    }
}
