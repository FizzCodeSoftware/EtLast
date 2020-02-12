namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Globalization;

    public static class Extensions
    {
        public static string FormatToString(this int number, string format = "D")
        {
            return number.ToString(format, CultureInfo.InvariantCulture);
        }

        public static string FormatToStringNoZero(this int number, string format = "D")
        {
            if (number == 0)
                return "";

            return FormatToString(number, format);
        }
    }
}
