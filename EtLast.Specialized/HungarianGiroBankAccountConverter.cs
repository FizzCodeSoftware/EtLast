namespace FizzCode.EtLast;

public sealed class HungarianGiroBankAccountConverter(string formatHint = null, IFormatProvider formatProviderHint = null) : StringConverter(formatHint, formatProviderHint)
{
    /// <summary>
    /// Default true.
    /// </summary>
    public required bool AutomaticallyAddHyphens { get; init; } = true;

    private static readonly int[] _checksumNumbers = [9, 7, 3, 1, 9, 7, 3];

    public override object Convert(object source)
    {
        var value = base.Convert(source) as string;
        return Convert(value, AutomaticallyAddHyphens);
    }

    public static string Convert(string value, bool automaticallyAddHyphens)
    {
        if (string.IsNullOrEmpty(value))
            return null;

        value = value
            .Replace(" ", "", StringComparison.Ordinal)
            .Replace("\n", "", StringComparison.Ordinal)
            .Trim();

        if (!Validate(value))
            return null;

        if (automaticallyAddHyphens && !value.Contains('-', StringComparison.InvariantCultureIgnoreCase))
        {
            return value.Length switch
            {
                16 => string.Concat(value.AsSpan(0, 8), "-", value.AsSpan(8, 8)),
                24 => string.Concat(value[..8], "-", value.Substring(8, 8), "-", value.Substring(16, 8)),
                _ => value,
            };
        }

        return value;
    }

    public static bool Validate(string value)
    {
        string[] parts;

        if (!value.Contains('-', StringComparison.InvariantCultureIgnoreCase))
        {
            switch (value.Length)
            {
                case 16:
                    parts = [value[..8], value.Substring(8, 8)];
                    break;
                case 24:
                    parts = [value[..8], value.Substring(8, 8), value.Substring(16, 8)];
                    break;
                default:
                    return false;
            }
        }
        else
        {
            parts = value.Split('-', StringSplitOptions.RemoveEmptyEntries);
            if ((parts.Length != 2 && parts.Length != 3) || parts.Any(x => x.Length != 8))
                return false;
        }

        var firstPart = parts[0]; // only the first part has checksum for ALL banks

        var digitSum = 0;
        for (var i = 0; i < 7; i++)
        {
            if (!int.TryParse(firstPart[i].ToString(CultureInfo.InvariantCulture), out var digit))
                return false;

            digitSum += digit * _checksumNumbers[i];
        }

        var checkSum = digitSum % 10;
        if (checkSum != 0)
            checkSum = 10 - checkSum;

        if (!int.TryParse(firstPart[7].ToString(CultureInfo.InvariantCulture), out var lastDigit))
            return false;

        if (lastDigit != checkSum)
            return false;

        return true;
    }
}
