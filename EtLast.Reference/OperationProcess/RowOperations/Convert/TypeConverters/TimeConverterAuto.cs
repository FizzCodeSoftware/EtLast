namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class TimeConverterAuto : TimeConverter
    {
        public string FormatHint { get; }
        public IFormatProvider FormatProviderHint { get; }

        public TimeConverterAuto(string formatHint, IFormatProvider formatProviderHint)
        {
            FormatHint = formatHint;
            FormatProviderHint = formatProviderHint;
        }

        public TimeConverterAuto(string formatHint)
        {
            FormatHint = formatHint;
            FormatProviderHint = null;
        }

        public TimeConverterAuto(IFormatProvider formatProviderHint)
        {
            FormatHint = null;
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
                    if (TimeSpan.TryParse(str, FormatProviderHint, out TimeSpan tsValue))
                    {
                        return tsValue;
                    }

                    if (DateTime.TryParse(str, FormatProviderHint, DateTimeStyles.AllowWhiteSpaces, out DateTime dtValue))
                    {
                        return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
                    }

                    if (TimeSpan.TryParse(str, FormatProviderHint, out tsValue))
                    {
                        return tsValue;
                    }

                    if (DateTime.TryParse(str, FormatProviderHint, DateTimeStyles.AllowWhiteSpaces, out dtValue))
                    {
                        return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
                    }
                }

                if (FormatHint != null)
                {
                    if (DateTime.TryParseExact(str, FormatHint, FormatProviderHint, DateTimeStyles.None, out DateTime dtValue))
                    {
                        return new TimeSpan(0, dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond);
                    }
                }
            }

            return null;
        }
    }
}