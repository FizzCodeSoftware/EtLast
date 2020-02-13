namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ColumnBasedRowEqualityComparer : IRowEqualityComparer
    {
        public string[] Columns { get; set; }
        public string[] ColumnsToIgnore { get; set; }

        public bool Equals(IRow leftRow, IRow rightRow)
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
                    if (!AbstractBaseRow.ValuesAreEqual(leftRow[column], rightRow[column]))
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

                    if (!AbstractBaseRow.ValuesAreEqual(kvp.Value, rightRow[kvp.Key]))
                        return false;
                }
            }

            return true;
        }

        public bool Equals(IRow leftRow, Dictionary<string, object> values)
        {
            if (Columns != null)
            {
                if (ColumnsToIgnore != null)
                    throw new ArgumentException(nameof(ColumnsToIgnore) + " can not be set if " + nameof(Columns) + " is set");

                foreach (var column in Columns)
                {
                    values.TryGetValue(column, out var rightValue);
                    if (!AbstractBaseRow.ValuesAreEqual(leftRow[column], rightValue))
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

                    values.TryGetValue(kvp.Key, out var rightValue);

                    if (!AbstractBaseRow.ValuesAreEqual(kvp.Value, rightValue))
                        return false;
                }
            }

            return true;
        }
    }
}