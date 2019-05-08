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
            if (InputGenerator == null)
                throw new ProcessParameterNullException(this, nameof(InputGenerator));
            var sw = Stopwatch.StartNew();

            foreach (var row in EvaluateInputProcess(sw))
                yield return row;

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