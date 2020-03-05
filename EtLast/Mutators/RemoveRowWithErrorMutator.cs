namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class RemoveRowWithErrorMutator : AbstractMutator
    {
        public RemoveRowWithErrorMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IEtlRow> MutateRow(IEtlRow row)
        {
            if (!row.HasError())
                yield return row;
        }
    }
}