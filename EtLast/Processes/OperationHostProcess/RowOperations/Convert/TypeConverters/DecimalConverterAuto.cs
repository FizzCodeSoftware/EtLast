namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class DecimalConverterAuto : DecimalConverter
    {
        public IFormatProvider FormatProviderHint { get; }
        public NumberStyles NumberStylesHint { get; }

        public DecimalConverterAuto(IFormatProvider formatProviderHint, NumberStyles numberStylesHint = NumberStyles.None)
        {
            FormatProviderHint = formatProviderHint;
            NumberStylesHint = numberStylesHint;
        }

        public override object Convert(object source)
        {
            var baseResult = base.Convert(source);
            if (baseResult != null)
                return baseResult;

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                    }
                }

                if (decimal.TryParse(str, NumberStylesHint, FormatProviderHint, out var value))
                {
                    return value;
                }
            }

            return null;
        }
    }
}