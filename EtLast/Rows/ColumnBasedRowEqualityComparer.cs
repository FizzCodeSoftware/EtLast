namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ColumnBasedRowEqualityComparer : IRowEqualityComparer
    {
        public string[] Columns { get; set; }
        public string[] ColumnsToIgnore { get; set; }

        public bool Compare(IRow leftRow, IRow rightRow)
        {
            if (Columns != null)
            {
                if (ColumnsToIgnore != null)
                    throw new ArgumentException(nameof(ColumnsToIgnore) + " can not be set if " + nameof(Columns) + " is set");

                foreach (var column in Columns)
                {
                    var leftValue = leftRow[column];
                    var rightValue = rightRow[column];
                    if (leftValue != null && rightValue != null)
                    {
                        if (!leftValue.Equals(rightValue))
                            return false;
                    }
                    else if (leftValue != rightValue)
                    {
                        return false;
                    }
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

                    var leftValue = kvp.Value;
                    var rightValue = rightRow[kvp.Key];
                    if (leftValue != null && rightValue != null)
                    {
                        if (leftValue.Equals(rightValue))
                            return false;
                    }
                    else if (leftValue != rightValue)
                    {
                        return false;
                    }
                }
            }

            return true;
        }
    }
}