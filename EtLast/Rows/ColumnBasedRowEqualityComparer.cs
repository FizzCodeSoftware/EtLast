namespace FizzCode.EtLast;

public sealed class ColumnBasedRowEqualityComparer : IRowEqualityComparer
{
    public required string[] Columns { get; init; }
    public string[] ColumnsToIgnore { get; init; }

    public bool Equals(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow)
    {
        if (leftRow == rightRow)
            return true;

        if (leftRow == null || rightRow == null)
            return false;

        if (ColumnsToIgnore != null)
            throw new ArgumentException(nameof(ColumnsToIgnore) + " can not be set if " + nameof(Columns) + " is set");

        foreach (var column in Columns)
        {
            if (!DefaultValueComparer.ValuesAreEqual(leftRow[column], rightRow[column]))
                return false;
        }

        return true;
    }
}
