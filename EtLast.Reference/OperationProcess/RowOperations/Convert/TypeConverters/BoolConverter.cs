namespace FizzCode.EtLast
{
    public class BoolConverter : ITypeConverter
    {
        public virtual object Convert(object source)
        {
            if (source is bool) return source;
            if (source is byte bv) return bv == 1;
            if (source is int iv) return iv == 1;
            if (source is long lv) return lv == 1;

            if (source is string str)
            {
                if (long.TryParse(str, out var value)) return value == 1;
            }

            return null;
        }
    }
}