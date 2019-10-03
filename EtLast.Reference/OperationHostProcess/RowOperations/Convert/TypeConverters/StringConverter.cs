namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class StringConverter : ITypeConverter
    {
        public string FormatHint { get; }
        public IFormatProvider FormatProviderHint { get; }

        public StringConverter(string formatHint = null, IFormatProvider formatProviderHint = null)
        {
            FormatHint = formatHint;
            FormatProviderHint = formatProviderHint;
        }

        public object Convert(object source)
        {
            if (source is string)
            {
                return source;
            }

            // todo: support all numerical values...

            if (source is int intValue)
            {
                try
                {
                    var value = FormatProviderHint != null
                        ? (FormatHint != null ? intValue.ToString(FormatHint, FormatProviderHint) : intValue.ToString(FormatProviderHint))
                        : (FormatHint != null ? intValue.ToString(FormatHint, CultureInfo.CurrentCulture) : intValue.ToString(CultureInfo.InvariantCulture));
                    return value;
                }
                catch
                {
                }
            }

            if (source is long longValue)
            {
                try
                {
                    var value = FormatProviderHint != null
                        ? (FormatHint != null ? longValue.ToString(FormatHint, FormatProviderHint) : longValue.ToString(FormatProviderHint))
                        : (FormatHint != null ? longValue.ToString(FormatHint, CultureInfo.CurrentCulture) : longValue.ToString(CultureInfo.InvariantCulture));
                    return value;
                }
                catch
                {
                }
            }

            if (source is double dblValue)
            {
                try
                {
                    var value = FormatProviderHint != null
                        ? (FormatHint != null ? dblValue.ToString(FormatHint, FormatProviderHint) : dblValue.ToString(FormatProviderHint))
                        : (FormatHint != null ? dblValue.ToString(FormatHint, CultureInfo.CurrentCulture) : dblValue.ToString(CultureInfo.InvariantCulture));
                    return value;
                }
                catch
                {
                }
            }

            if (source is float fltValue)
            {
                try
                {
                    var value = FormatProviderHint != null
                        ? (FormatHint != null ? fltValue.ToString(FormatHint, FormatProviderHint) : fltValue.ToString(FormatProviderHint))
                        : (FormatHint != null ? fltValue.ToString(FormatHint, CultureInfo.CurrentCulture) : fltValue.ToString(CultureInfo.InvariantCulture));
                    return value;
                }
                catch
                {
                }
            }

            try
            {
                var asGenericConvertedString = FormatProviderHint != null ? System.Convert.ToString(source, FormatProviderHint) : System.Convert.ToString(source, CultureInfo.CurrentCulture);
                return asGenericConvertedString;
            }
            catch
            {
            }

            try
            {
                var asToString = source.ToString();
                return asToString;
            }
            catch
            {
            }

            return null;
        }
    }
}