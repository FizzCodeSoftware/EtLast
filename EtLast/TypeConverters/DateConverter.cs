namespace FizzCode.EtLast;

public class DateConverter : ITypeConverter, ITextConverter
{
    public DateTime? EpochDate { get; init; }

    // https://en.wikipedia.org/wiki/Epoch_(reference_date)
    public static DateTime ExcelEpochDate { get; } = new DateTime(1899, 12, 30);
    public static DateTime UnixEpochDate { get; } = new DateTime(1970, 1, 1);

    public virtual object Convert(object source)
    {
        if (source is DateTime dt)
            return dt.Date;

        if (source is DateTimeOffset dto)
            return dto.DateTime.Date;

        if (source is string stringValue)
        {
            if (EpochDate != null && double.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var dv))
            {
                source = dv;
            }
            else if (DateTime.TryParse(stringValue, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var value))
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

    public object Convert(TextBuilder source)
    {
        var span = source.GetContentAsSpan();
        if (EpochDate != null && double.TryParse(span, NumberStyles.Any, CultureInfo.InvariantCulture, out var dv))
        {
            try
            {
                return EpochDate.Value.AddDays(dv).Date;
            }
            catch
            {
            }
        }
        else if (DateTime.TryParse(span, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var value))
        {
            return value.Date;
        }

        return null;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DateConverterFluent
{
    public static ReaderColumn AsDate(this ReaderColumn column) => column.WithTypeConverter(new DateConverter());
    public static IConvertMutatorBuilder_NullStrategy ToDate(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new DateConverter());
}