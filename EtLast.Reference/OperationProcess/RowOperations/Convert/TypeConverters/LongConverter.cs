namespace FizzCode.EtLast
{
    public class LongConverter : ITypeConverter
    {
        public string[] RemoveSubString { get; set; }

        public virtual object Convert(object source)
        {
            if (source is long) return source;
            if (source is float fv) return System.Convert.ToInt64(fv);
            if (source is double dv) return System.Convert.ToInt64(dv);
            if (source is int iv) return System.Convert.ToInt64(iv);
            if (source is uint uiv) return System.Convert.ToInt64(uiv);

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, string.Empty);
                    }
                }

                if (long.TryParse(str, out var value)) return value;
                if (float.TryParse(str, out var sfv)) return System.Convert.ToInt64(sfv);
                else if (double.TryParse(str, out var sdv)) return System.Convert.ToInt64(sdv);
                else if (int.TryParse(str, out var siv)) return System.Convert.ToInt64(siv);
                else if (uint.TryParse(str, out var suiv)) return System.Convert.ToInt64(suiv);
            }

            return null;
        }
    }
}