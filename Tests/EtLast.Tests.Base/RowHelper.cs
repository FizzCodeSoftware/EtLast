namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;

    public static class RowHelper
    {
        public static IRow CreateRow(object[] rowElements)
        {
            var row = new DictionaryRow();
            row.Init(null, -1, rowElements.Length);

            for (var i = 0; i < rowElements.Length; i += 2)
            {
                row[rowElements[i] as string] = rowElements[i + 1];
            }

            return row;
        }

        public static List<IRow> CreateRows(string[] columns, object[][] data)
        {
            var rows = new List<IRow>();

            for (var i = 0; i < data.Length; i++)
            {
                var row = new DictionaryRow();
                row.Init(null, -1, columns.Length);

                var columnNumber = 0;
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
            var result = resultArray.ToList();

            if (rows == null || rows.Count < 2)
                return rows;

            var first = rows[0];
            IOrderedEnumerable<IRow> order = null;
            foreach (var kvp in first.Values)
            {
                if (!(kvp.Value is EtlRowError))
                {
                    order = order is null ? result.OrderBy(r => r[kvp.Key]) : order.ThenBy(r => r[kvp.Key]);
                }
            }

            return order.ToList();
        }

        public static List<IRow> CreateRows(params object[][] rowsData)
        {
            var rows = new List<IRow>();
            foreach (var rowElements in rowsData)
            {
                rows.Add(CreateRow(rowElements));
            }

            return rows;
        }
    }
}
