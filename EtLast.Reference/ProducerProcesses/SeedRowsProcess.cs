namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public class SeedRowsProcess : AbstractBaseProducerProcess
    {
        public int Count { get; set; }
        public string[] Columns { get; set; }

        public SeedRowsProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;

            Context.Log(LogSeverity.Information, this, "started");
            var sw = Stopwatch.StartNew();

            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

                var inputRows = InputProcess.Evaluate(this);
                var rowCount = 0;
                foreach (var row in inputRows)
                {
                    rowCount++;
                    yield return row;
                }

                Context.Log(LogSeverity.Debug, this, "fetched and returned {RowCount} rows from {InputProcess} in {Elapsed}", rowCount, InputProcess.Name, sw.Elapsed);
            }

            Context.Log(LogSeverity.Debug, this, "returning generated random rows");
            var resultCount = 0;
            foreach (var id in Enumerable.Range(0, Count))
            {
                var row = Context.CreateRow();

                var inputRow = Columns.Select(col => CreateRandomObject(id, col)).ToArray();
                for (int i = 0; i < Math.Min(Columns.Length, inputRow.Length); i++)
                {
                    row.SetValue(Columns[i], inputRow[i], this);
                }

                resultCount++;
                yield return row;
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} random generated rows in {Elapsed}", resultCount, sw.Elapsed);
        }

        private static readonly Random Rnd = new Random();

        public static object CreateRandomObject(int id, string column)
        {
            if (string.Equals(column, "id", StringComparison.OrdinalIgnoreCase))
            {
                return id;
            }

            if (column.EndsWith("id", StringComparison.OrdinalIgnoreCase))
            {
                return Rnd.Next(1, 10000);
            }

            if (column.EndsWith("datetime", StringComparison.OrdinalIgnoreCase))
            {
                return new DateTime(2000, 1, 1).AddDays(Rnd.Next(1, 365 * 30)).AddHours(Rnd.Next(25)).AddMinutes(Rnd.Next(60)).AddSeconds(Rnd.Next(60));
            }

            if (column.EndsWith("date", StringComparison.OrdinalIgnoreCase))
            {
                return new DateTime(2000, 1, 1).AddDays(Rnd.Next(1, 365 * 30));
            }

            if (column.EndsWith("time", StringComparison.OrdinalIgnoreCase))
            {
                return new TimeSpan(0, Rnd.Next(24), Rnd.Next(60), Rnd.Next(60), Rnd.Next(1000));
            }

            if (column.EndsWith("code2", StringComparison.OrdinalIgnoreCase))
            {
                var text = string.Empty;
                for (int i = 0; i < 2; i++)
                {
                    text += (char)(65 + Rnd.Next(26));
                }

                return text;
            }

            if (column.EndsWith("name", StringComparison.OrdinalIgnoreCase))
            {
                var n = Rnd.Next(3, 10);
                var text = string.Empty;
                for (int i = 0; i < n; i++)
                {
                    text += (char)(65 + Rnd.Next(26));
                }

                return text;
            }

            switch (column.GetHashCode() % 10)
            {
                case 8:
                    return new DateTime(2000, 1, 1).AddDays(Rnd.Next(1, 365 * 30));
                case 9:
                    var n = Rnd.Next(3, 10);
                    var text = string.Empty;
                    for (int i = 0; i < n; i++)
                    {
                        text += (char)(65 + Rnd.Next(26));
                    }

                    return text;
                default:
                    return Rnd.Next(1, 100000);
            }
        }
    }
}