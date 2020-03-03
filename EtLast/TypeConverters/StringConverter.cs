namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class StringConverter : ITypeConverter
    {
        public string FormatHint { get; }
        public IFormatProvider FormatProvider { get; }

        /// <summary>
        /// Default false.
        /// </summary>
        public bool TrimStartEnd { get; set; } = false;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool RemoveLineBreaks { get; set; } = false;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool RemoveSpaces { get; set; } = false;

        public StringConverter(IFormatProvider formatProvider = null)
        {
            FormatProvider = formatProvider;
        }

        public StringConverter(string format, IFormatProvider formatProvider = null)
        {
            FormatHint = format;
            FormatProvider = formatProvider;
        }

        public virtual object Convert(object source)
        {
            var result = ConvertToString(source);
            if (!string.IsNullOrEmpty(result))
            {
                if (TrimStartEnd)
                {
                    result = result.Trim();
                }

                if (RemoveLineBreaks)
                {
                    result = result
                        .Replace("\r", "", StringComparison.InvariantCultureIgnoreCase)
                        .Replace("\n", "", StringComparison.InvariantCultureIgnoreCase);
                }

                if (RemoveSpaces)
                {
                    result = result
                        .Replace(" ", "", StringComparison.InvariantCultureIgnoreCase);
                }
            }

            return result;
        }

        protected string ConvertToString(object source)
        {
            if (source is string stringValue)
            {
                return stringValue;
            }

            if (source is IFormattable formattable)
            {
                try
                {
                    return formattable.ToString(FormatHint, FormatProvider ?? CultureInfo.InvariantCulture);
                }
                catch
                {
                }
            }

            try
            {
                return source.ToString();
            }
            catch
            {
            }

            return null;
        }
    }
}