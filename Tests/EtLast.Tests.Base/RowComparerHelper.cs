namespace FizzCode.EtLast.Tests.Base
{
    using System.Text;
    using FizzCode.EtLast;
    
    public static class RowComparerHelper
    {
        public static string CompareMessage(IRow row1, IRow row2)
        {
            if (row1 == null && row2 != null
                || row1 != null && row2 == null)
                return null;

            StringBuilder sb = new StringBuilder();

            foreach (var kvp in row1.Values)
            {
                if (kvp.Value == null && row2[kvp.Key] == null
                    || kvp.Value.Equals(row2[kvp.Key]))
                    sb.Append("  ");
                else
                    sb.Append("! ");

                sb.Append(kvp.Key);
                sb.Append(": ");
                sb.Append(kvp.Value);
                sb.Append(" | ");
                sb.AppendLine(row2[kvp.Key]?.ToString());
            }

            return sb.ToString();
        }
    }
}
