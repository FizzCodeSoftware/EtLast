namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public sealed class ColumnBasedRowEqualityComparer : IRowEqualityComparer
    {
        public string[] Columns { get; set; }
        public string[] ColumnsToIgnore { get; set; }

        public bool Equals(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow)
        {
            if (leftRow == rightRow)
                return true;

            if (leftRow == null || rightRow == null)
                return false;

            if (Columns != null)
            {
                if (ColumnsToIgnore != null)
                    throw new ArgumentException(nameof(ColumnsToIgnore) + " can not be set if " + nameof(Columns) + " is set");

                foreach (var column in Columns)
                {
                    if (!DefaultValueComparer.ValuesAreEqual(leftRow[column], rightRow[column]))
                        return false;
                }
            }
            else
            {
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
            }

            return true;
        }
    }
}