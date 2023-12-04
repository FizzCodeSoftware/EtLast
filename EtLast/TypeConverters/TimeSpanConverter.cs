namespace FizzCode.EtLast;

public class TimeSpanConverter : ITypeConverter
{
    public virtual object Convert(object source)
    {
        if (source is TimeSpan)
            return source;

        if (source is DateTime dt)
            return dt.TimeOfDay;

        if (source is string stringValue)
        {
            if (TimeSpan.TryParse(stringValue, CultureInfo.InvariantCulture, out var tsValue))
            {
                return tsValue;
            }

            if (DateTime.TryParse(stringValue, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var dtValue))
            {
                return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
            }
        }

        return null;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class TimeSpanConverterFluent
{
    public static ReaderColumn AsTimeSpan(this ReaderColumn column) => column.WithTypeConverter(new TimeSpanConverter());
    public static IConvertMutatorBuilder_NullStrategy ToTimeSpan(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new TimeSpanConverter());
}