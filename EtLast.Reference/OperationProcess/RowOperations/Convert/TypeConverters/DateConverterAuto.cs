namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class DateConverterAuto : DateConverter
    {
        public string[] FormatHints { get; }
        public IFormatProvider FormatProviderHint { get; }

        public DateConverterAuto(IFormatProvider formatProviderHint)
            : base()
        {
            FormatHints = null;
            FormatProviderHint = formatProviderHint;
        }

        public DateConverterAuto(string formatHint)
            : base()
        {
            FormatHints = new[] { formatHint };
            FormatProviderHint = null;
        }

        public DateConverterAuto(string[] formatHints)
            : base()
        {
            FormatHints = formatHints;
            FormatProviderHint = null;
        }

        public DateConverterAuto(string formatHint, IFormatProvider formatProviderHint)
            : base()
        {
            FormatHints = new[] { formatHint };
            FormatProviderHint = formatProviderHint;
        }

        public DateConverterAuto(string[] formatHints, IFormatProvider formatProviderHint)
            : base()
        {
            FormatHints = formatHints;
            FormatProviderHint = formatProviderHint;
        }

        public override object Convert(object source)
        {
            var baseResult = base.Convert(source);
            if (baseResult != null)
                return baseResult;

            if (source is string str)
            {
                if (FormatHints != null && FormatProviderHint != null)
                {
                    if (DateTime.TryParseExact(str, FormatHints, FormatProviderHint, DateTimeStyles.None, out var value))
                    {
                        return value;
                    }
                }

                if (FormatHints != null && FormatProviderHint == null)
                {
                    if (DateTime.TryParseExact(str, FormatHints, null, DateTimeStyles.None, out var value))
                    {
                        return value;
                    }
                }

                if (FormatProviderHint != null)
                {
                    if (DateTime.TryParse(str, FormatProviderHint, DateTimeStyles.AllowWhiteSpaces, out var value))
                    {
                        return value;
                    }
                }
            }

            return null;
        }
    }
}