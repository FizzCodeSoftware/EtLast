namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class DateTimeConverterAuto : DateTimeConverter
    {
        public string FormatHint { get; }
        public IFormatProvider FormatProviderHint { get; }

        public DateTimeConverterAuto(string formatHint)
            : base()
        {
            FormatHint = formatHint;
        }

        public DateTimeConverterAuto(IFormatProvider formatProviderHint)
            : base()
        {
            FormatHint = null;
            FormatProviderHint = formatProviderHint;
        }

        public DateTimeConverterAuto(string formatHint, IFormatProvider formatProviderHint)
            : base()
        {
            FormatHint = formatHint;
            FormatProviderHint = formatProviderHint;
        }

        public override object Convert(object source)
        {
            var baseResult = base.Convert(source);
            if (baseResult != null) return baseResult;

            if (source is string str)
            {
                if (FormatProviderHint != null)
                {
                    if (DateTime.TryParse(str, FormatProviderHint, DateTimeStyles.AllowWhiteSpaces, out var value))
                    {
                        return value;
                    }
                }

                if (FormatHint != null)
                {
                    if (DateTime.TryParseExact(str, FormatHint, FormatProviderHint, DateTimeStyles.None, out var value))
                    {
                        return value;
                    }
                }
            }

            return null;
        }
    }
}