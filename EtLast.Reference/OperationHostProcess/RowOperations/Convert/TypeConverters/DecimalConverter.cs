namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class DecimalConverter : ITypeConverter
    {
        public string[] RemoveSubString { get; set; }
        public bool UseInvariantCulture { get; }

        public DecimalConverter(bool useInvariantCulture = false)
        {
            UseInvariantCulture = useInvariantCulture;
        }

        public virtual object Convert(object source)
        {
            if (source is decimal)
                return source;

            // whole numbers
            if (source is sbyte sbv)
                return System.Convert.ToDecimal(sbv);

            if (source is byte bv)
                return System.Convert.ToDecimal(bv);

            if (source is short sv)
                return System.Convert.ToDecimal(sv);

            if (source is ushort usv)
                return System.Convert.ToDecimal(usv);

            if (source is int iv)
                return System.Convert.ToDecimal(iv);

            if (source is uint uiv)
                return System.Convert.ToDecimal(uiv);

            if (source is long lv && lv >= decimal.MinValue && lv <= decimal.MaxValue)
                return System.Convert.ToDecimal(lv);

            if (source is ulong ulv && ulv <= decimal.MaxValue)
                return System.Convert.ToDecimal(ulv);

            // decimal values
            if (source is double dv)
                return System.Convert.ToDecimal(dv);

            if (source is float fv)
                return System.Convert.ToDecimal(fv);

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                    }
                }

                var numberFormatInfo = NumberFormatInfo.CurrentInfo;

                if (UseInvariantCulture)
                    numberFormatInfo = CultureInfo.InvariantCulture.NumberFormat;

                if (decimal.TryParse(str, NumberStyles.Number, numberFormatInfo, out var value))
                    return value;
                else if (double.TryParse(str, NumberStyles.Number, numberFormatInfo, out var dfv))
                    return dfv;
                else if (float.TryParse(str, NumberStyles.Number, numberFormatInfo, out var sfv))
                    return System.Convert.ToDouble(sfv);
                else if (int.TryParse(str, NumberStyles.Number, numberFormatInfo, out var siv))
                    return System.Convert.ToDouble(siv);
            }

            return null;
        }
    }
}