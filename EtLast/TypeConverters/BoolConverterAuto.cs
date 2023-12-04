namespace FizzCode.EtLast;

public class BoolConverterAuto : BoolConverter
{
    public string KnownTrueString { get; init; }
    public string KnownFalseString { get; init; }

    public override object Convert(object source)
    {
        if (source is string stringValue)
        {
            switch (stringValue.ToUpperInvariant().Trim())
            {
                case "TRUE":
                case "YES":
                case "1":
                    return true;
                case "FALSE":
                case "NO":
                case "0":
                    return false;
            }

            if (KnownTrueString != null && string.Equals(stringValue.Trim(), KnownTrueString, StringComparison.InvariantCultureIgnoreCase))
                return true;

            if (KnownFalseString != null && string.Equals(stringValue.Trim(), KnownFalseString, StringComparison.InvariantCultureIgnoreCase))
                return false;
        }

        return base.Convert(source);
    }

    public override object Convert(TextBuilder source)
    {
        var span = source.GetContentAsSpan().Trim();
        if (span.Equals("TRUE", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("YES", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("1", StringComparison.InvariantCultureIgnoreCase))
        {
            return true;
        }

        if (span.Equals("FALSE", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("NO", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("0", StringComparison.InvariantCultureIgnoreCase))
        {
            return false;
        }

        if (KnownTrueString != null && span.Equals(KnownTrueString, StringComparison.InvariantCultureIgnoreCase))
            return true;

        if (KnownFalseString != null && span.Equals(KnownFalseString, StringComparison.InvariantCultureIgnoreCase))
            return false;

        return null;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class BoolConverterAutoFluent
{
    public static ReaderColumn AsBoolAuto(this ReaderColumn column) => column.WithTypeConverter(new BoolConverterAuto());
    public static IConvertMutatorBuilder_NullStrategy ToBoolAuto(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new BoolConverterAuto());
}