namespace FizzCode.EtLast
{
    using System;

    public class DateTimeOffsetConverter : ITypeConverter
    {
        public virtual object Convert(object source)
        {
            if (source is DateTimeOffset)
                return source;

            if (source is string str)
            {
                if (DateTimeOffset.TryParse(str, out var value))
                {
                    return value;
                }
            }

            return null;
        }
    }
}