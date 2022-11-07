namespace FizzCode.EtLast;

public class DateTimeOffsetConverter : ITypeConverter
{
    public virtual object Convert(object source)
    {
        if (source is DateTimeOffset)
            return source;

        if (source is string stringValue)
        {
            if (DateTimeOffset.TryParse(stringValue, out var value))
            {
                return value;
            }
        }

        return null;
    }
}
