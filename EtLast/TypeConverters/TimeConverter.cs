namespace FizzCode.EtLast;

public class TimeConverter : ITypeConverter
{
    public virtual object Convert(object source)
    {
        if (source is TimeSpan)
            return source;

        if (source is DateTime dt)
            return dt.TimeOfDay;

        if (source is string str)
        {
            if (TimeSpan.TryParse(str, CultureInfo.InvariantCulture, out var tsValue))
            {
                return tsValue;
            }

            if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var dtValue))
            {
                return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
            }
        }

        return null;
    }
}
