namespace FizzCode.EtLast;

public class BoolConverterAuto : BoolConverter
{
    public string KnownTrueString { get; set; }
    public string KnownFalseString { get; set; }

    public override object Convert(object source)
    {
        if (source is string stringValue)
        {
            switch (stringValue.ToUpperInvariant().Trim())
            {
                case "TRUE":
                case "YES":
                    return true;
                case "FALSE":
                case "NO":
                    return false;
            }

            if (KnownTrueString != null && string.Equals(stringValue.Trim(), KnownTrueString, StringComparison.InvariantCultureIgnoreCase))
                return true;

            if (KnownFalseString != null && string.Equals(stringValue.Trim(), KnownFalseString, StringComparison.InvariantCultureIgnoreCase))
                return false;
        }

        return base.Convert(source);
    }

    public override object Convert(TextReaderStringBuilder source)
    {
        var span = source.GetContentAsSpan().Trim();
        if (span.Equals("TRUE", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("YES", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("1", StringComparison.InvariantCultureIgnoreCase))
            return true;

        if (span.Equals("FALSE", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("NO", StringComparison.InvariantCultureIgnoreCase) ||
            span.Equals("0", StringComparison.InvariantCultureIgnoreCase))
            return false;

        if (KnownTrueString != null && span.Equals(KnownTrueString, StringComparison.InvariantCultureIgnoreCase))
            return true;

        if (KnownFalseString != null && span.Equals(KnownFalseString, StringComparison.InvariantCultureIgnoreCase))
            return false;

        return null;
    }
}