namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public class SeedRowsProcess : AbstractProducerProcess
    {
        public int Count { get; set; }
        public string[] Columns { get; set; }

        public SeedRowsProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            if (Count == 0)
                throw new InvalidProcessParameterException(this, nameof(Count), Count, "value must be greater than zero");
        }

        protected override IEnumerable<IRow> Produce(Stopwatch startedOn)
        {
            Context.Log(LogSeverity.Debug, this, "returning random generated rows");
            foreach (var id in Enumerable.Range(0, Count))
            {
                var row = Context.CreateRow();

                var inputRow = Columns.Select(col => CreateRandomObject(id, col)).ToArray();
                for (var i = 0; i < Math.Min(Columns.Length, inputRow.Length); i++)
                {
                    row.SetValue(Columns[i], inputRow[i], this);
                }

                yield return row;
            }
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
                var text = "";
                for (var i = 0; i < 2; i++)
                {
                    text += (char)(65 + Rnd.Next(26));
                }

                return text;
            }

            if (column.EndsWith("name", StringComparison.OrdinalIgnoreCase))
            {
                var n = Rnd.Next(3, 10);
                var text = "";
                for (var i = 0; i < n; i++)
                {
                    text += (char)(65 + Rnd.Next(26));
                }

                return text;
            }

            switch (column.GetHashCode(StringComparison.InvariantCultureIgnoreCase) % 10)
            {
                case 8:
                    return new DateTime(2000, 1, 1).AddDays(Rnd.Next(1, 365 * 30));
                case 9:
                    var n = Rnd.Next(3, 10);
                    var text = "";
                    for (var i = 0; i < n; i++)
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