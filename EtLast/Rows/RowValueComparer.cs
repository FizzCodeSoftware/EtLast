namespace FizzCode.EtLast
{
    public static class RowValueComparer
    {
        public static bool ValuesAreEqual(object leftValue, object rightValue)
        {
            if (leftValue == null && rightValue == null)
                return true;

            if ((leftValue != null && rightValue == null) || (leftValue == null && rightValue != null))
                return false;

            if (leftValue is EtlRowError e1 && rightValue is EtlRowError e2)
            {
                return (e1.OriginalValue != null && e2.OriginalValue != null)
                    ? e1.OriginalValue.Equals(e2.OriginalValue)
                    : e1.OriginalValue != e2.OriginalValue;
            }

            return leftValue.Equals(rightValue);
        }
    }
}