namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Creates <see cref="IRow"/>s from <see cref="InputRows"/>.
    /// Use to create test input rows, or to inject litaral-like data.
    /// </summary>
    /// <remarks>Do not use to read into <see cref="InputRows"/> from a data source.</remarks>
    public class CreateRowsProcess : AbstractProducerProcess
    {
        public string[] Columns { get; set; }
        public List<object[]> InputRows { get; set; } = new List<object[]>();
        public int BatchSize { get; set; } = 100;

        public CreateRowsProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
        {
            if (InputRows == null)
                throw new ProcessParameterNullException(this, nameof(InputRows));
        }

        protected override IEnumerable<IRow> Produce()
        {
            Context.Log(LogSeverity.Debug, this, "returning pre-defined rows");

            foreach (var inputRow in InputRows)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                var row = Context.CreateRow();

                for (var i = 0; i < Math.Min(Columns.Length, inputRow.Length); i++)
                {
                    row.SetValue(Columns[i], inputRow[i], this);
                }

                yield return row;
            }
        }
    }
}