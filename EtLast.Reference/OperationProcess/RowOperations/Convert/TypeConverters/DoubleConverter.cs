namespace FizzCode.EtLast
{
    public class DoubleConverter : ITypeConverter
    {
        public DoubleConverter()
        {
        }

        public virtual object Convert(object source)
        {
            if (source is double) return source;
            if (source is float fv) return System.Convert.ToDouble(fv);
            if (source is int iv) return System.Convert.ToDouble(iv);

            if (source is string str)
            {
                if (double.TryParse(str, out double value)) return value;
                if (float.TryParse(str, out float sfv)) return System.Convert.ToDouble(sfv);
                else if (int.TryParse(str, out int siv)) return System.Convert.ToDouble(siv);
            }

            return null;
        }
    }
}