namespace FizzCode.EtLast;

using System;
using System.Globalization;

public class DateConverter : ITypeConverter
{
    public DateTime? EpochDate { get; set; }

    // https://en.wikipedia.org/wiki/Epoch_(reference_date)
    public static DateTime ExcelEpochDate { get; } = new DateTime(1899, 12, 30);
    public static DateTime UnixEpochDate { get; } = new DateTime(1970, 1, 1);

    public virtual object Convert(object source)
    {
        if (source is DateTime dt)
            return dt.Date;

        if (source is DateTimeOffset dto)
            return dto.DateTime.Date;

        if (source is string str)
        {
            if (EpochDate != null && double.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var dv))
            {
                source = dv;
            }
            else if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var value))
            {
                return value.Date;
            }
        }

        if (EpochDate != null)
        {
            if (source is double doubleValue)
            {
                try
                {
                    return EpochDate.Value.AddDays(doubleValue).Date;
                }
                catch
                {
                }
            }
            else if (source is int intValue)
            {
                try
                {
                    return EpochDate.Value.AddDays(intValue).Date;
                }
                catch
                {
                }
            }
        }

        return null;
    }
}
