namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate bool CustomMutatorDelegate(IEvaluable process, IRow row);

    public class CustomMutator : AbstractMutator
    {
        public CustomMutatorDelegate Then { get; set; }

        public CustomMutator(ITopic topic, string name)
            : base(topic, name)
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