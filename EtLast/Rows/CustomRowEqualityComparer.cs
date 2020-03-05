namespace FizzCode.EtLast
{
    using System;

    public delegate bool CustomRowEqualityComparerDelegate(IReadOnlyRow leftRow, IReadOnlyRow rightRow);

    public class CustomRowEqualityComparer : IRowEqualityComparer
    {
        public CustomRowEqualityComparerDelegate ComparerDelegate { get; set; }

        public bool Equals(IReadOnlyRow leftRow, IReadOnlyRow rightRow)
        {
            if (ComparerDelegate == null)
                throw new ArgumentException(nameof(ComparerDelegate) + " can not be null");

            return ComparerDelegate.Invoke(leftRow, rightRow);
        }
    }
}