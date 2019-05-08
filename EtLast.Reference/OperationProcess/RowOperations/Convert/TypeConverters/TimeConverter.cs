namespace FizzCode.EtLast
{
    using System;

    public class TimeConverter : ITypeConverter
    {
        public TimeConverter()
        {
        }

        public virtual object Convert(object source)
        {
            if (source is TimeSpan)
                return source;
            if (source is DateTime dt)
                return new TimeSpan(0, dt.Hour, dt.Minute, dt.Second, dt.Millisecond);

            if (source is string str)
            {
                if (TimeSpan.TryParse(str, out var tsValue))
                {
                    return tsValue;
                }

                if (DateTime.TryParse(str, out var dtValue))
                {
                    return new TimeSpan(dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
                }

                if (int.TryParse(str, out var iv))
                {
                    source = iv;
                }
                else if (double.TryParse(str, out var dv))
                {
                    source = dv;
                }
            }

            if (source is int intValue)
            {
                return new TimeSpan(intValue);
            }

            if (source is double doubleValue)
            {
                try
                {
                    var value = new TimeSpan(System.Convert.ToInt64(60d * 60d * 24d * 10000000d * doubleValue));
                    return value;
                }
                catch { }
            }

            if (source is float floatValue)
            {
                try
                {
                    var value = new TimeSpan(System.Convert.ToInt64(60d * 60d * 24d * 10000000d * floatValue));
                    return value;
                }
                catch { }
            }

            return null;
        }
    }
}