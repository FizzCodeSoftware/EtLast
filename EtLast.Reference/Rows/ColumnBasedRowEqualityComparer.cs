namespace FizzCode.EtLast.Rows
{
    public class ColumnBasedRowEqualityComparer : IRowEqualityComparer
    {
        public string[] Columns { get; set; }

        public bool Compare(IRow leftRow, IRow rightRow)
        {
            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    var leftValue = leftRow[column];
                    var rightValue = rightRow[column];
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
            else
            {
                if (leftRow.ColumnCount != rightRow.ColumnCount)
                    return false;

                foreach (var kvp in leftRow.Values)
                {
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