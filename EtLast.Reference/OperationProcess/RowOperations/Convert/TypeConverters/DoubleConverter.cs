namespace FizzCode.EtLast
{
    using System.Globalization;

    public class DoubleConverter : ITypeConverter
    {
        public bool UseInvariantCluture { get; }

        public DoubleConverter(bool useInvariantCluture = false)
        {
            UseInvariantCluture = useInvariantCluture;
        }

        public virtual object Convert(object source)
        {
            if (source is double)
                return source;
            if (source is float fv)
                return System.Convert.ToDouble(fv);
            if (source is int iv)
                return System.Convert.ToDouble(iv);

            if (source is string str)
            {
                var numberFormatInfo = NumberFormatInfo.CurrentInfo;
                if (UseInvariantCluture)
                    numberFormatInfo = CultureInfo.InvariantCulture.NumberFormat;

                if (double.TryParse(str, NumberStyles.Number, numberFormatInfo, out var value))
                    return value;
                if (float.TryParse(str, NumberStyles.Number, numberFormatInfo, out var sfv))
                    return System.Convert.ToDouble(sfv);
                else if (int.TryParse(str, NumberStyles.Number, numberFormatInfo, out var siv))
                    return System.Convert.ToDouble(siv);
            }

            return null;
        }
    }
}