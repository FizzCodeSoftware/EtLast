namespace FizzCode.EtLast;

using System;
using System.Globalization;
using System.Linq;

public sealed class HungarianGiroBankAccountConverter : StringConverter
{
    /// <summary>
    /// Default true.
    /// </summary>
    public bool AutomaticallyAddHyphens { get; init; } = true;

    private static readonly int[] _checkSumNumbers = new[] { 9, 7, 3, 1, 9, 7, 3 };

    public HungarianGiroBankAccountConverter(string formatHint = null, IFormatProvider formatProviderHint = null)
        : base(formatHint, formatProviderHint)
    {
        RemoveSpaces = true;
        RemoveLineBreaks = true;
        TrimStartEnd = true;
    }

    public override object Convert(object source)
    {
        var taxNr = base.Convert(source) as string;

        return Convert(taxNr, AutomaticallyAddHyphens);
    }

    public static string Convert(string accountNr, bool automaticallyAddHyphens)
    {
        if (string.IsNullOrEmpty(accountNr))
            return null;

        if (!Validate(accountNr))
            return null;

        if (automaticallyAddHyphens && !accountNr.Contains('-', StringComparison.InvariantCultureIgnoreCase))
        {
            return accountNr.Length switch
            {
                16 => accountNr.Substring(0, 8) + "-" + accountNr.Substring(8, 8),
                24 => accountNr.Substring(0, 8) + "-" + accountNr.Substring(8, 8) + "-" + accountNr.Substring(16, 8),
                _ => accountNr,
            };
        }

        return accountNr;
    }

    public static bool Validate(string accountNr)
    {
        string[] parts;

        if (!accountNr.Contains('-', StringComparison.InvariantCultureIgnoreCase))
        {
            switch (accountNr.Length)
            {
                case 16:
                    parts = new[] { accountNr.Substring(0, 8), accountNr.Substring(8, 8) };
                    break;
                case 24:
                    parts = new[] { accountNr.Substring(0, 8), accountNr.Substring(8, 8), accountNr.Substring(16, 8) };
                    break;
                default:
                    return false;
            }
        }
        else
        {
            parts = accountNr.Split('-', StringSplitOptions.RemoveEmptyEntries);
            if ((parts.Length != 2 && parts.Length != 3) || parts.Any(x => x.Length != 8))
                return false;
        }

        var firstPart = parts[0]; // only the first part has checksum for ALL banks

        var digitSum = 0;
        for (var i = 0; i < 7; i++)
        {
            if (!int.TryParse(firstPart[i].ToString(CultureInfo.InvariantCulture), out var digit))
                return false;

            digitSum += digit * _checkSumNumbers[i];
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
