namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class EnumerableImporter : AbstractProducer
    {
        public EvaluateDelegate InputGenerator { get; set; }

        public EnumerableImporter(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputGenerator == null)
                throw new ProcessParameterNullException(this, nameof(InputGenerator));
        }

        protected override IEnumerable<IRow> Produce()
        {
            var inputRows = InputGenerator.Invoke(this);
            foreach (var row in inputRows)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                yield return row;
            }
        }
    }
}