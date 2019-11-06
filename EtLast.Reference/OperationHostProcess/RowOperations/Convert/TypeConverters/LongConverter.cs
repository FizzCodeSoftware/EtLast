using System;

namespace FizzCode.EtLast
{
    public class LongConverter : ITypeConverter
    {
        public string[] RemoveSubString { get; set; }

        public virtual object Convert(object source)
        {
            if (source is long)
                return source;

            // smaller whole numbers
            if (source is sbyte sbv)
                return System.Convert.ToInt64(sbv);

            if (source is byte bv)
                return System.Convert.ToInt64(bv);

            if (source is short sv)
                return System.Convert.ToInt64(sv);

            if (source is ushort usv)
                return System.Convert.ToInt64(usv);

            if (source is int iv)
                return System.Convert.ToInt64(iv);

            if (source is uint uiv)
                return System.Convert.ToInt64(uiv);

            // larger whole numbers
            if (source is ulong ulv && ulv <= long.MaxValue)
                return System.Convert.ToInt64(ulv);

            // decimal values
            if (source is float fv && fv >= long.MinValue && fv <= long.MaxValue)
                return System.Convert.ToInt64(fv);

            if (source is double dv && dv >= long.MinValue && dv <= long.MaxValue)
                return System.Convert.ToInt64(dv);

            if (source is decimal dcv && dcv >= long.MinValue && dcv <= long.MaxValue)
                return System.Convert.ToInt64(dcv);

            if (source is bool boolv)
                return boolv ? 1L : 0L;

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                    }
                }

                if (long.TryParse(str, out var value))
                    return value;

                if (float.TryParse(str, out var sfv))
                    return System.Convert.ToInt64(sfv);
                else if (double.TryParse(str, out var sdv))
                    return System.Convert.ToInt64(sdv);
                else if (int.TryParse(str, out var siv))
                    return System.Convert.ToInt64(siv);
                else if (uint.TryParse(str, out var suiv))
                    return System.Convert.ToInt64(suiv);
            }

            return null;
        }
    }
}