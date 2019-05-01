namespace FizzCode.EtLast
{
    using System.Globalization;

    public class DecimalConverter : ITypeConverter
    {
        public string[] RemoveSubString { get; set; }
        public bool UseInvariantCluture { get; set; }

        public DecimalConverter(bool useInvariantCluture = false)
        {
            UseInvariantCluture = useInvariantCluture;
        }

        public virtual object Convert(object source)
        {
            if (source is decimal) return source;
            if (source is double dv) return System.Convert.ToDecimal(dv);
            if (source is float fv) return System.Convert.ToDecimal(fv);
            if (source is int iv) return System.Convert.ToDecimal(iv);

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, string.Empty);
                    }
                }

                var numberFormatInfo = NumberFormatInfo.CurrentInfo;
                if (UseInvariantCluture)
                    numberFormatInfo = CultureInfo.InvariantCulture.NumberFormat;

                if (decimal.TryParse(str, NumberStyles.Number, numberFormatInfo, out decimal value)) return value;
                else if (double.TryParse(str, NumberStyles.Number, numberFormatInfo, out double dfv)) return dfv;
                else if (float.TryParse(str, NumberStyles.Number, numberFormatInfo, out float sfv)) return System.Convert.ToDouble(sfv);
                else if (int.TryParse(str, NumberStyles.Number, numberFormatInfo, out int siv)) return System.Convert.ToDouble(siv);
            }

            return null;
        }
    }
}