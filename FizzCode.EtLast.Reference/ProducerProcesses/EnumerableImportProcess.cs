namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;

    public class EnumerableImportProcess : AbstractBaseProducerProcess
    {
        public EvaluateDelegate InputGenerator { get; set; }

        public EnumerableImportProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (InputGenerator == null) throw new ProcessParameterNullException(this, nameof(InputGenerator));
            var sw = Stopwatch.StartNew();

            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

                var rows = InputProcess.Evaluate(this);
                var count = 0;
                foreach (var row in rows)
                {
                    count++;
                    yield return row;
                }

                Context.Log(LogSeverity.Debug, this, "fetched and returned {RowCount} rows from {InputProcess} in {Elapsed}", count, InputProcess.Name, sw.Elapsed);
            }

            Context.Log(LogSeverity.Information, this, "evaluating input generator");

            var inputRows = InputGenerator.Invoke(this);
            var rowCount = 0;
            foreach (var row in inputRows)
            {
                rowCount++;
                yield return row;
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", rowCount, sw.Elapsed);
        }
    }
}