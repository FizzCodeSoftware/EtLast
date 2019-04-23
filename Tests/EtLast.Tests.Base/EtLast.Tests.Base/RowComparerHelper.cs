namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
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
                if (kvp.Value.Equals(row2[kvp.Key]))
                    sb.Append("  ");
                else
                    sb.Append("! ");

                sb.Append(kvp.Key);
                sb.Append(": ");
                sb.Append(kvp.Value);
                sb.Append(" | ");
                sb.AppendLine(row2[kvp.Key].ToString());
            }

            return sb.ToString();
        }

        public static IRow CreateRow(object[] rowElements)
        {
            DictionaryRow row = new DictionaryRow();
            row.Init(null, -1, rowElements.Length);

            for (int i = 0; i < rowElements.Length; i += 2)
            {
                row[rowElements[i] as string] = rowElements[i + 1];
            }

            return row;
        }

        public static List<IRow> CreateRows(string[] columns, object[][] data)
        {
            List<IRow> rows = new List<IRow>();

            for (int i = 0; i < data.Length; i++)
            {
                DictionaryRow row = new DictionaryRow();
                row.Init(null, -1, columns.Length);

                int columnNumber = 0;
                foreach (var column in columns)
                {
                    if (data[i].Length <= columnNumber)
                        break;

                    row[column] = data[i][columnNumber++];
                }
            }

            return rows;
        }
    }
}
