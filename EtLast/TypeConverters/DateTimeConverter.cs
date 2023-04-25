namespace FizzCode.EtLast;

public class DateTimeConverter : ITypeConverter, ITextConverter
{
    public DateTime? EpochDate { get; init; }

    public virtual object Convert(object source)
    {
        if (source is DateTime dt)
            return dt;

        if (source is string stringValue)
        {
            if (EpochDate != null && double.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var dv))
            {
                source = dv;
            }
            else if (DateTime.TryParse(stringValue, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var value))
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

    public object Convert(TextBuilder source)
    {
        var span = source.GetContentAsSpan();
        if (EpochDate != null && double.TryParse(span, NumberStyles.Any, CultureInfo.InvariantCulture, out var dv))
        {
            try
            {
                return EpochDate.Value.AddDays(dv);
            }
            catch
            {
            }
        }
        else if (DateTime.TryParse(span, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var value))
        {
            return value;
        }

        return null;
    }
}