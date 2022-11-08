namespace FizzCode.EtLast;

public class DoubleConverter : ITypeConverter, ITextConverter
{
    public string[] RemoveSubString { get; set; }

    public virtual object Convert(object source)
    {
        if (source is double)
            return source;

        if (source is string stringValue)
        {
            if (RemoveSubString != null)
            {
                foreach (var subStr in RemoveSubString)
                {
                    stringValue = stringValue.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
                }
            }

            if (double.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        // whole numbers
        if (source is sbyte sbv)
            return System.Convert.ToDouble(sbv);

        if (source is byte bv)
            return System.Convert.ToDouble(bv);

        if (source is short sv)
            return System.Convert.ToDouble(sv);

        if (source is ushort usv)
            return System.Convert.ToDouble(usv);

        if (source is int iv)
            return System.Convert.ToDouble(iv);

        if (source is uint uiv)
            return System.Convert.ToDouble(uiv);

        if (source is long lv && lv >= double.MinValue && lv <= double.MaxValue)
            return System.Convert.ToDouble(lv);

        if (source is ulong ulv && ulv <= double.MaxValue)
            return System.Convert.ToDouble(ulv);

        // decimal values
        if (source is float fv)
            return System.Convert.ToDouble(fv);

        if (source is decimal dcv)
            return System.Convert.ToDouble(dcv);

        return null;
    }

    public object Convert(TextReaderStringBuilder source)
    {
        if (RemoveSubString != null)
        {
            var stringValue = source.GetContentAsString();
            foreach (var subStr in RemoveSubString)
            {
                stringValue = stringValue.Replace(subStr, "", StringComparison.InvariantCultureIgnoreCase);
            }

            if (double.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        else
        {
            if (double.TryParse(source.GetContentAsSpan(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                return value;
        }

        return null;
    }
}
