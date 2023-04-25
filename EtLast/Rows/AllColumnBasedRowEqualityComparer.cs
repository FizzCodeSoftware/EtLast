namespace FizzCode.EtLast;

public sealed class AllColumnBasedRowEqualityComparer : IRowEqualityComparer
{
    public string[] ColumnsToIgnore { get; init; }

    public bool Equals(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow)
    {
        if (leftRow == rightRow)
            return true;

        if (leftRow == null || rightRow == null)
            return false;

        var columnsToIgnore = ColumnsToIgnore != null
            ? new HashSet<string>(ColumnsToIgnore)
            : null;

        foreach (var kvp in leftRow.Values)
        {
            if (columnsToIgnore?.Contains(kvp.Key) == true)
                continue;

            if (!DefaultValueComparer.ValuesAreEqual(kvp.Value, rightRow[kvp.Key]))
                return false;
        }

        foreach (var kvp in rightRow.Values)
        {
            if (columnsToIgnore?.Contains(kvp.Key) == true)
                continue;

            if (!DefaultValueComparer.ValuesAreEqual(kvp.Value, leftRow[kvp.Key]))
                return false;
        }

        return true;
    }
}
