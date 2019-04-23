namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    
    public static class RowHelper
    {
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

        public static List<IRow> OrderRows(List<IRow> rows)
        {
            var resultArray = new IRow[rows.Count];
            rows.CopyTo(resultArray);
            List<IRow> result = resultArray.ToList();

            if (rows == null
                || rows.Count < 2)
                return rows;

            var first = rows[0];
            IOrderedEnumerable<IRow> order = result.OrderBy(r => r[first.Values.First().Key]);
            foreach (var kvp in first.Values.Skip(1))
            {
                order = order.ThenBy(r => r[kvp.Key]);
            }

            return order.ToList();
        }
    }
}
