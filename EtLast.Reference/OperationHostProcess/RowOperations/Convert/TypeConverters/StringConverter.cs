namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class StringConverter : ITypeConverter
    {
        public string FormatHint { get; }
        public IFormatProvider FormatProviderHint { get; }

        /// <summary>
        /// Default false.
        /// </summary>
        public bool TrimStartEnd { get; set; } = false;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool RemoveLineBreaksFromMiddle { get; set; } = false;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool RemoveSpacesFromMiddle { get; set; } = false;

        public StringConverter(string formatHint = null, IFormatProvider formatProviderHint = null)
        {
            FormatHint = formatHint;
            FormatProviderHint = formatProviderHint;
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

                if (RemoveLineBreaksFromMiddle)
                {
                    result = result
                        .Replace("\r", "", StringComparison.InvariantCultureIgnoreCase)
                        .Replace("\n", "", StringComparison.InvariantCultureIgnoreCase);
                }

                if (RemoveSpacesFromMiddle)
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
                    return formattable.ToString(FormatHint, FormatProviderHint ?? CultureInfo.CurrentCulture);
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