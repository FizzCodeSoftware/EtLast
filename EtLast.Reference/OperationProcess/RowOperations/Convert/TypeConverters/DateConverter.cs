namespace FizzCode.EtLast
{
    using System;

    public class DateConverter : ITypeConverter
    {
        public DateTime? EpochDate { get; set; }

        // https://en.wikipedia.org/wiki/Epoch_(reference_date)
        public static DateTime ExcelEpochDate { get; } = new DateTime(1899, 12, 30);
        public static DateTime UnixEpochDate { get; } = new DateTime(1970, 1, 1);

        public virtual object Convert(object source)
        {
            if (source is DateTime) return source;
            if (source is DateTimeOffset dto) return dto.DateTime;

            if (source is string str)
            {
                if (DateTime.TryParse(str, out var value))
                {
                    return value.Date;
                }

                if (EpochDate != null)
                {
                    if (double.TryParse(str, out var dv))
                    {
                        source = dv;
                    }
                    else if (int.TryParse(str, out var iv))
                    {
                        source = iv;
                    }
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
                    catch { }
                }
                else if (source is int intValue)
                {
                    try
                    {
                        return EpochDate.Value.AddDays(intValue);
                    }
                    catch { }
                }
            }

            return null;
        }
    }
}