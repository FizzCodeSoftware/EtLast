namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate bool CustomMutatorDelegate(IEvaluable process, IRow row);

    public class CustomMutator : AbstractMutator
    {
        public CustomMutatorDelegate Then { get; set; }

        public CustomMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (Then.Invoke(this, row))
                yield return row;
        }

        protected override void ValidateMutator()
        {
            if (Then == null)
                throw new ProcessParameterNullException(this, nameof(Then));
        }
    }
}