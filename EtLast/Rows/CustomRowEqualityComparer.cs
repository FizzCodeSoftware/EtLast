namespace FizzCode.EtLast
{
    using System;

    public delegate bool CustomRowEqualityComparerDelegate(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow);

    public class CustomRowEqualityComparer : IRowEqualityComparer
    {
        public CustomRowEqualityComparerDelegate ComparerDelegate { get; set; }

        public bool Equals(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow)
        {
            if (ComparerDelegate == null)
                throw new ArgumentException(nameof(ComparerDelegate) + " can not be null");

            return ComparerDelegate.Invoke(leftRow, rightRow);
        }
    }
}