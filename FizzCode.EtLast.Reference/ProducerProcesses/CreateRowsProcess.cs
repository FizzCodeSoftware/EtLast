namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public class CreateRowsProcess : AbstractBaseProducerProcess
    {
        public string[] Columns { get; set; }
        public List<object[]> InputRows { get; set; } = new List<object[]>();
        public int BatchSize { get; set; } = 100;

        public CreateRowsProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (InputRows == null) throw new ProcessParameterNullException(this, nameof(InputRows));
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

            Context.Log(LogSeverity.Debug, this, "returning pre-defined rows");

            var resultCount = 0;
            foreach (var inputRow in InputRows)
            {
                var row = Context.CreateRow();

                for (int i = 0; i < Math.Min(Columns.Length, inputRow.Length); i++)
                {
                    row.SetValue(Columns[i], inputRow[i], this);
                }

                resultCount++;
                yield return row;
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
        }
    }
}