namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class SeedRowsOperation : AbstractRowOperation
    {
        public int RowCount { get; set; }
        public string[] Columns { get; set; }

        public override void Apply(IRow row)
        {
            var rnd = new Random();

            var buffer = new List<IRow>();
            for (int id = 0; id < RowCount; id++)
            {
                var newRow = Process.Context.CreateRow();
                newRow.CurrentOperation = this;

                var inputRow = Columns.Select(col => CreateRandomObject(id, col, rnd)).ToArray();
                for (int i = 0; i < Math.Min(Columns.Length, inputRow.Length); i++)
                {
                    newRow.SetValue(Columns[i], inputRow[i], this);
                }

                buffer.Add(newRow);

                if (buffer.Count >= 1000)
                {
                    Process.AddRows(buffer, this);
                    buffer.Clear();
                }
            }

            if (buffer.Count > 0)
            {
                Process.AddRows(buffer, this);
                buffer.Clear();
            }
        }

        public static object CreateRandomObject(int id, string column, Random rnd)
        {
            if (string.Equals(column, "id", StringComparison.OrdinalIgnoreCase))
            {
                return id;
            }

            if (column.EndsWith("id", StringComparison.OrdinalIgnoreCase))
            {
                return rnd.Next(1, 10000);
            }

            if (column.EndsWith("datetime", StringComparison.OrdinalIgnoreCase))
            {
                return new DateTime(2000, 1, 1).AddDays(rnd.Next(1, 365 * 30)).AddHours(rnd.Next(25)).AddMinutes(rnd.Next(60)).AddSeconds(rnd.Next(60));
            }

            if (column.EndsWith("date", StringComparison.OrdinalIgnoreCase))
            {
                return new DateTime(2000, 1, 1).AddDays(rnd.Next(1, 365 * 30));
            }

            if (column.EndsWith("time", StringComparison.OrdinalIgnoreCase))
            {
                return new TimeSpan(0, rnd.Next(24), rnd.Next(60), rnd.Next(60), rnd.Next(1000));
            }

            if (column.EndsWith("name", StringComparison.OrdinalIgnoreCase))
            {
                var n = rnd.Next(3, 10);
                var text = string.Empty;
                for (int i = 0; i < n; i++)
                {
                    text += (char)(65 + rnd.Next(26));
                }

                return text;
            }

            switch (column.GetHashCode() % 10)
            {
                case 8:
                    return new DateTime(2000, 1, 1).AddDays(rnd.Next(1, 365 * 30));
                case 9:
                    var n = rnd.Next(3, 10);
                    var text = string.Empty;
                    for (int i = 0; i < n; i++)
                    {
                        text += (char)(65 + rnd.Next(26));
                    }

                    return text;
                default:
                    return rnd.Next(1, 100000);
            }
        }

        public override void Prepare()
        {
            if (RowCount <= 0) throw new InvalidOperationParameterException(this, nameof(RowCount), RowCount, "value must be greater than 0");
            if (Columns == null || Columns.Length == 0) throw new OperationParameterNullException(this, nameof(Columns));
        }
    }
}