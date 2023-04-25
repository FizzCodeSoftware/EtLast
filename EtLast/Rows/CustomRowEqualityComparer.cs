namespace FizzCode.EtLast;

public delegate bool CustomRowEqualityComparerDelegate(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow);

public sealed class CustomRowEqualityComparer : IRowEqualityComparer
{
    public required CustomRowEqualityComparerDelegate ComparerDelegate { get; init; }

    public bool Equals(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow)
    {
        if (ComparerDelegate == null)
            throw new ArgumentException(nameof(ComparerDelegate) + " can not be null");

        return ComparerDelegate.Invoke(leftRow, rightRow);
    }
}
