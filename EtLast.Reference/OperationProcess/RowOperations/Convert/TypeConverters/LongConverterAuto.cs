namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class LongConverterAuto : LongConverter
    {
        public IFormatProvider FormatProviderHint { get; }
        public NumberStyles NumberStylesHint { get; }

        public LongConverterAuto(IFormatProvider formatProviderHint, NumberStyles numberStylesHint = NumberStyles.None)
        {
            FormatProviderHint = formatProviderHint;
            NumberStylesHint = numberStylesHint;
        }

        public override object Convert(object source)
        {
            var baseResult = base.Convert(source);
            if (baseResult != null) return baseResult;

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, string.Empty);
                    }
                }

                if (long.TryParse(str, NumberStylesHint, FormatProviderHint, out long value))
                {
                    return value;
                }
            }

            return null;
        }
    }
}