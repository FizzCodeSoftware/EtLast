namespace FizzCode.EtLast.Tests.Base
{
    using System.Text;
    using FizzCode.EtLast;

    public static class RowComparerHelper
    {
        public static string CompareMessage(IRow row1, IRow row2, RowComparer.RowComparerMode rowComparerMode = RowComparer.RowComparerMode.Test)
        {
            if ((row1 == null && row2 != null) || (row1 != null && row2 == null))
                return null;

            var sb = new StringBuilder();
            var rowComparer = new RowComparer(rowComparerMode);

            foreach (var kvp in row1.Values)
            {
                if (rowComparer.Equals(kvp, row2))
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