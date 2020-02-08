namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class SeedRowsProcess : AbstractProducerProcess
    {
        public int Count { get; set; }
        public string[] Columns { get; set; }

        public SeedRowsProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            if (Count == 0)
                throw new InvalidProcessParameterException(this, nameof(Count), Count, "value must be greater than zero");
        }

        protected override IEnumerable<IRow> Produce()
        {
            Context.Log(LogSeverity.Debug, this, "returning random generated rows");
            foreach (var id in Enumerable.Range(0, Count))
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                var initialValues = Columns.Select(col => new KeyValuePair<string, object>(col, CreateRandomObject(id, col)));

                var row = Context.CreateRow(this, initialValues);
                yield return row;
            }
        }

        private static readonly Random _rnd = new Random();

        public static object CreateRandomObject(int id, string column)
        {
            if (string.Equals(column, "id", StringComparison.OrdinalIgnoreCase))
            {
                return id;
            }

            if (column.EndsWith("id", StringComparison.OrdinalIgnoreCase))
            {
                return _rnd.Next(1, 10000);
            }

            if (column.EndsWith("datetime", StringComparison.OrdinalIgnoreCase))
            {
                return new DateTime(2000, 1, 1).AddDays(_rnd.Next(1, 365 * 30)).AddHours(_rnd.Next(25)).AddMinutes(_rnd.Next(60)).AddSeconds(_rnd.Next(60));
            }

            if (column.EndsWith("date", StringComparison.OrdinalIgnoreCase))
            {
                return new DateTime(2000, 1, 1).AddDays(_rnd.Next(1, 365 * 30));
            }

            if (column.EndsWith("time", StringComparison.OrdinalIgnoreCase))
            {
                return new TimeSpan(0, _rnd.Next(24), _rnd.Next(60), _rnd.Next(60), _rnd.Next(1000));
            }

            if (column.EndsWith("code2", StringComparison.OrdinalIgnoreCase))
            {
                var text = "";
                for (var i = 0; i < 2; i++)
                {
                    text += (char)(65 + _rnd.Next(26));
                }

                return text;
            }

            if (column.EndsWith("name", StringComparison.OrdinalIgnoreCase))
            {
                var n = _rnd.Next(3, 10);
                var text = "";
                for (var i = 0; i < n; i++)
                {
                    text += (char)(65 + _rnd.Next(26));
                }

                return text;
            }

            switch (column.GetHashCode(StringComparison.InvariantCultureIgnoreCase) % 10)
            {
                case 8:
                    return new DateTime(2000, 1, 1).AddDays(_rnd.Next(1, 365 * 30));
                case 9:
                    var n = _rnd.Next(3, 10);
                    var text = "";
                    for (var i = 0; i < n; i++)
                    {
                        text += (char)(65 + _rnd.Next(26));
                    }

                    return text;
                default:
                    return _rnd.Next(1, 100000);
            }
        }
    }
}