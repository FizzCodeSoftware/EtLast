namespace FizzCode.EtLast
{
    public class IntConverter : ITypeConverter
    {
        public string[] RemoveSubString { get; set; }

        public IntConverter()
        {
        }

        public virtual object Convert(object source)
        {
            if (source is int)
                return source;
            if (source is float fv)
                return System.Convert.ToInt32(fv);
            if (source is double dv)
                return System.Convert.ToInt32(dv);

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, string.Empty);
                    }
                }

                if (int.TryParse(str, out var value))
                    return value;
                if (float.TryParse(str, out var sfv))
                    return System.Convert.ToInt32(sfv);
                else if (double.TryParse(str, out var sdv))
                    return System.Convert.ToInt32(sdv);
            }

            return null;
        }
    }
}