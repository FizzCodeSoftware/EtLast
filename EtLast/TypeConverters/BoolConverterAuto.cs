namespace FizzCode.EtLast;

public class BoolConverterAuto : BoolConverter
{
    public string KnownTrueString { get; set; }
    public string KnownFalseString { get; set; }

    public override object Convert(object source)
    {
        if (source is string str)
        {
            switch (str.ToUpperInvariant().Trim())
            {
                case "TRUE":
                case "YES":
                    return true;
                case "FALSE":
                case "NO":
                    return false;
            }

            if (KnownTrueString != null && string.Equals(str.Trim(), KnownTrueString, StringComparison.InvariantCultureIgnoreCase))
                return true;

            if (KnownFalseString != null && string.Equals(str.Trim(), KnownFalseString, StringComparison.InvariantCultureIgnoreCase))
                return false;
        }

        return base.Convert(source);
    }
}
