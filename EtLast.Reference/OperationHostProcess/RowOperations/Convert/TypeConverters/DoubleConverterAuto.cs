namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class DoubleConverterAuto : DoubleConverter
    {
        public IFormatProvider FormatProviderHint { get; }
        public NumberStyles NumberStylesHint { get; }

        public bool UseOnlyProvidedHints { get; }

        public DoubleConverterAuto(IFormatProvider formatProviderHint, NumberStyles numberStylesHint = NumberStyles.None, bool useOnlyProvidedHints = false)
        {
            FormatProviderHint = formatProviderHint;
            NumberStylesHint = numberStylesHint;
            UseOnlyProvidedHints = useOnlyProvidedHints;
        }

        public override object Convert(object source)
        {
            if (!UseOnlyProvidedHints)
            {
                var baseResult = base.Convert(source);
                if (baseResult != null)
                    return baseResult;
            }

            if (source is string str)
            {
                if (double.TryParse(str, NumberStylesHint, FormatProviderHint, out var value))
                {
                    return value;
                }
            }

            return null;
        }
    }
}