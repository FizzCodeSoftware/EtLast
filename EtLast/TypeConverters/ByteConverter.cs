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

            if (source is sbyte sbv && sbv >= byte.MinValue)
                return System.Convert.ToByte(sbv);

            // larger whole numbers
            if (source is short sv && sv >= byte.MinValue && sv <= byte.MaxValue)
                return System.Convert.ToByte(sv);

            if (source is ushort usv && usv <= byte.MaxValue)
                return System.Convert.ToByte(usv);

            if (source is int iv && iv >= byte.MinValue && iv <= byte.MaxValue)
                return System.Convert.ToByte(iv);

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
            }

            return null;
        }
    }
}