namespace FizzCode.EtLast;

public class StringConverter : ITypeConverter
{
    public string FormatHint { get; }
    public IFormatProvider FormatProvider { get; }

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
        string result = null;
        if (source is not string stringValue)
        {
            if (source is IFormattable formattable)
            {
                try
                {
                    result = formattable.ToString(FormatHint, FormatProvider ?? CultureInfo.InvariantCulture);
                }
                catch
                {
                }
            }
            else
            {
                try
                {
                    result = source.ToString();
                }
                catch
                {
                }
            }
        }
        else
        {
            result = stringValue;
        }

        return result;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class StringConverterFluent
{
    public static ReaderColumn AsString(this ReaderColumn column, IFormatProvider formatProvider = null) => column.WithTypeConverter(new StringConverter(formatProvider));
    public static ReaderColumn AsString(this ReaderColumn column, string format, IFormatProvider formatProvider = null) => column.WithTypeConverter(new StringConverter(format, formatProvider));
}