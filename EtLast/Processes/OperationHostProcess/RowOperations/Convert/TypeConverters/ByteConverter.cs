namespace FizzCode.EtLast
{
    using System;

    public class ByteConverter : ITypeConverter
    {
        public string[] RemoveSubString { get; set; }

        public virtual object Convert(object source)
        {
            if (source is byte)
                return source;

            // smaller whole numbers
            if (source is sbyte sbv)
                return System.Convert.ToByte(sbv);

            if (source is byte bv)
                return System.Convert.ToByte(bv);

            if (source is short sv)
                return System.Convert.ToByte(sv);

            if (source is ushort usv)
                return System.Convert.ToByte(usv);

            // larger whole numbers
            if (source is uint uiv && uiv <= byte.MaxValue)
                return System.Convert.ToByte(uiv);

            if (source is long lv && lv >= byte.MinValue && lv <= byte.MaxValue)
                return System.Convert.ToByte(lv);

            if (source is ulong ulv && ulv <= byte.MaxValue)
                return System.Convert.ToByte(ulv);

            // decimal values
            if (source is float fv && fv >= byte.MinValue && fv <= byte.MaxValue)
                return System.Convert.ToByte(fv);

            if (source is double dv && dv >= byte.MinValue && dv <= byte.MaxValue)
                return System.Convert.ToByte(dv);

            if (source is decimal dcv && dcv >= byte.MinValue && dcv <= byte.MaxValue)
                return System.Convert.ToByte(dcv);

            if (source is bool boolv)
                return boolv ? (byte)1 : (byte)0;

            if (source is string str)
            {
                if (RemoveSubString != null)
                {
                    foreach (var subStr in RemoveSubString)
                    {
                        str = str.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                    }
                }

                if (byte.TryParse(str, out var value))
                    return value;

                if (float.TryParse(str, out var sfv))
                    return System.Convert.ToByte(sfv);
                else if (double.TryParse(str, out var sdv))
                    return System.Convert.ToByte(sdv);
            }

            return null;
        }
    }
}