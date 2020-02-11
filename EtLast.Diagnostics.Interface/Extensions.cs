namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Globalization;

    public static class Extensions
    {
        public static string ToStringNoZero(this int number, string format = "D")
        {
            if (number == 0)
                return "";

            return number.ToString(format, CultureInfo.InvariantCulture);
        }
    }
}
