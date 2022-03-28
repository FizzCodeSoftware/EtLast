namespace FizzCode.EtLast;

public delegate bool CustomRowEqualityComparerDelegate(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow);

public sealed class CustomRowEqualityComparer : IRowEqualityComparer
{
    public CustomRowEqualityComparerDelegate ComparerDelegate { get; set; }

    public bool Equals(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow)
    {
        if (ComparerDelegate == null)
            throw new ArgumentException(nameof(ComparerDelegate) + " can not be null");

        return ComparerDelegate.Invoke(leftRow, rightRow);
    }
}
