namespace FizzCode.EtLast
{
    using System;

    public delegate bool CustomRowEqualityComparerDelegate(IValueCollection leftRow, IValueCollection rightRow);

    public class CustomRowEqualityComparer : IRowEqualityComparer
    {
        public CustomRowEqualityComparerDelegate ComparerDelegate { get; set; }

        public bool Equals(IValueCollection leftRow, IValueCollection rightRow)
        {
            if (ComparerDelegate == null)
                throw new ArgumentException(nameof(ComparerDelegate) + " can not be null");

            return ComparerDelegate.Invoke(leftRow, rightRow);
        }
    }
}