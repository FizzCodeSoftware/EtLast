namespace FizzCode.EtLast.Tests
{
    using System.Text;
    using FizzCode.EtLast;

    public static class RowComparerHelper
    {
        public static string CompareMessage(IRow row1, IRow row2)
        {
            if ((row1 == null && row2 != null) || (row1 != null && row2 == null))
                return null;

            var sb = new StringBuilder();

            foreach (var kvp in row1.Values)
            {
                if (RowValueComparer.ValuesAreEqual(kvp.Value, row2[kvp.Key]))
                    sb.Append("  ");
                else
                    sb.Append("! ");

                sb.Append(kvp.Key)
                    .Append(": ")
                    .Append(kvp.Value)
                    .Append(" | ")
                    .AppendLine(row2[kvp.Key]?.ToString());
            }

            return sb.ToString();
        }
    }
}