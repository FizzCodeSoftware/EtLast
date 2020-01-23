namespace FizzCode.EtLast
{
    public class ByteArrayConverter : ITypeConverter
    {
        public virtual object Convert(object source)
        {
            if (source is byte[])
                return source;

            return null;
        }
    }
}