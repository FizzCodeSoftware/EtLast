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
            var initialValues = new List<KeyValuePair<string, object>>();
            for (var i = 0; i < rowElements.Length; i += 2)
            {
                initialValues.Add(new KeyValuePair<string, object>(rowElements[i] as string, rowElements[i + 1]));
            }

            row.Init(null, null, -1, initialValues);
            return row;
        }

        public static List<IRow> CreateRows(string[] columns, object[][] data)
        {
            var rows = new List<IRow>();

            for (var i = 0; i < data.Length; i++)
            {
                var initialValues = new List<KeyValuePair<string, object>>();
                var columnNumber = 0;
                foreach (var column in columns)
                {
                    if (data[i].Length <= columnNumber)
                        break;

                    initialValues.Add(new KeyValuePair<string, object>(column, data[i][columnNumber++]));
                }

                var row = new DictionaryRow();
                row.Init(null, null, -1, initialValues);
            }

            return rows;
        }

        public static List<IRow> OrderRows(List<IRow> rows)
        {
            if (rows == null || rows.Count < 2)
                return rows;

            var result = rows.ToList();

            var first = rows[0];
            IOrderedEnumerable<IRow> order = null;
            foreach (var kvp in first.Values)
            {
                if (!(kvp.Value is EtlRowError))
                {
                    order = order is null
                        ? result.OrderBy(r => r[kvp.Key])
                        : order.ThenBy(r => r[kvp.Key]);
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