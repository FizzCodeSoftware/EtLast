namespace FizzCode.EtLast;

public static class DefaultValueComparer
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

        if (leftValue is byte[] ba1 && rightValue is byte[] ba2)
        {
            return ba1.SequenceEqual(ba2);
        }

        if (leftValue is System.Drawing.Color c1 && rightValue is System.Drawing.Color c2)
        {
            return c1.ToArgb().Equals(c2.ToArgb());
        }

        return leftValue.Equals(rightValue);
    }
}
