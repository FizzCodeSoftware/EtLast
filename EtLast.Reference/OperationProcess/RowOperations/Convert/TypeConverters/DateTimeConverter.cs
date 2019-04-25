namespace FizzCode.EtLast
{
    using System;

    public class DateTimeConverter : ITypeConverter
    {
        public DateTime? EpochDate { get; set; }

        public virtual object Convert(object source)
        {
            if (source is DateTime) return source;

            if (source is string str)
            {
                if (DateTime.TryParse(str, out DateTime value))
                {
                    return value;
                }

                if (EpochDate != null)
                {
                    if (double.TryParse(str, out double dv))
                    {
                        source = dv;
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

                if (source is long longValue)
                {
                    try
                    {
                        return EpochDate.Value.AddSeconds(longValue);
                    }
                    catch { }
                }
            }

            return null;
        }
    }
}