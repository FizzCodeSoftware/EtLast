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
    public class CreateRowsProcess : AbstractBaseProducerProcess
    {
        public string[] Columns { get; set; }
        public List<object[]> InputRows { get; set; } = new List<object[]>();
        public int BatchSize { get; set; } = 100;

        public CreateRowsProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IExecutionBlock caller = null)
        {
            Caller = caller;
            if (InputRows == null)
                throw new ProcessParameterNullException(this, nameof(InputRows));

            var startedOn = Stopwatch.StartNew();

            foreach (var row in EvaluateInputProcess(startedOn))
                yield return row;

            Context.Log(LogSeverity.Debug, this, "returning pre-defined rows");

            var resultCount = 0;
            foreach (var inputRow in InputRows)
            {
                var row = Context.CreateRow();

                for (var i = 0; i < Math.Min(Columns.Length, inputRow.Length); i++)
                {
                    row.SetValue(Columns[i], inputRow[i], this);
                }

                resultCount++;
                yield return row;
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, startedOn.Elapsed);
        }
    }
}