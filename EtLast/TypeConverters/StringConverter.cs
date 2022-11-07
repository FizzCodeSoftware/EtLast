namespace FizzCode.EtLast;

public class StringConverter : ITypeConverter
{
    public string FormatHint { get; }
    public IFormatProvider FormatProvider { get; }

    /// <summary>
    /// Default false.
    /// </summary>
    public bool TrimStartEnd { get; set; }

    /// <summary>
    /// Default false.
    /// </summary>
    public bool RemoveLineBreaks { get; set; }

    /// <summary>
    /// Default false.
    /// </summary>
    public bool RemoveSpaces { get; set; }

    /// <summary>
    /// Default false.
    /// </summary>
    public bool ReplaceEmptyStringWithNull { get; set; }

    public StringConverter(IFormatProvider formatProvider = null)
    {
        FormatProvider = formatProvider;
    }

    public StringConverter(string format, IFormatProvider formatProvider = null)
    {
        FormatHint = format;
        FormatProvider = formatProvider;
    }

    public virtual object Convert(object source)
    {
        var result = source is string stringValue
            ? stringValue
            : ConvertToString(source);

        if (!string.IsNullOrEmpty(result))
        {
            if (RemoveLineBreaks)
            {
                result = result
                    .Replace("\r", "", StringComparison.InvariantCultureIgnoreCase)
                    .Replace("\n", "", StringComparison.InvariantCultureIgnoreCase);
            }

            if (TrimStartEnd)
            {
                result = result.Trim();
            }

            if (RemoveSpaces)
            {
                result = result
                    .Replace(" ", "", StringComparison.InvariantCultureIgnoreCase);
            }

            if (ReplaceEmptyStringWithNull && result == string.Empty)
            {
                result = null;
            }
        }

        return result;
    }

    protected string ConvertToString(object source)
    {
        if (source is string stringValue)
        {
            return stringValue;
        }

        if (source is IFormattable formattable)
        {
            try
            {
                return formattable.ToString(FormatHint, FormatProvider ?? CultureInfo.InvariantCulture);
            }
            catch
            {
            }
        }

        try
        {
            return source.ToString();
        }
        catch
        {
        }

        return null;
    }
}
