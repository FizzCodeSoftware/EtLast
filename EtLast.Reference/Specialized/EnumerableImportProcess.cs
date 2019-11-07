namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;

    public class EnumerableImportProcess : AbstractProducerProcess
    {
        public EvaluateDelegate InputGenerator { get; set; }

        public EnumerableImportProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            if (InputGenerator == null)
                throw new ProcessParameterNullException(this, nameof(InputGenerator));
        }

        protected override IEnumerable<IRow> Produce(Stopwatch startedOn)
        {
            Context.Log(LogSeverity.Information, this, "evaluating input generator");

            var inputRows = InputGenerator.Invoke(this);
            foreach (var row in inputRows)
            {
                yield return row;
            }
        }
    }
}