namespace FizzCode.EtLast;

using System;
using System.Globalization;

public class DateTimeConverter : ITypeConverter
{
    public DateTime? EpochDate { get; set; }

    public virtual object Convert(object source)
    {
        if (source is DateTime dt)
            return dt;

        if (source is string str)
        {
            if (EpochDate != null && double.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var dv))
            {
                source = dv;
            }
            else if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var value))
            {
                return value;
            }
        }

        if (EpochDate != null)
        {
            if (source is double doubleValue)
            {
                try
                {
                    return EpochDate.Value.AddDays(doubleValue);
                }
                catch
                {
                }
            }

            if (source is long longValue)
            {
                try
                {
                    return EpochDate.Value.AddSeconds(longValue);
                }
                catch
                {
                }
            }
        }

        return null;
    }
}
