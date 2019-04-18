namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class DoubleConverterAuto : DoubleConverter
    {
        public IFormatProvider FormatProviderHint { get; }
        public NumberStyles NumberStylesHint { get; }

        public DoubleConverterAuto(IFormatProvider formatProviderHint, NumberStyles numberStylesHint = NumberStyles.None)
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
                if (double.TryParse(str, NumberStylesHint, FormatProviderHint, out double value))
                {
                    return value;
                }
            }

            return null;
        }
    }
}