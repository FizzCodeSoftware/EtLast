namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class RemoveRowMutator : AbstractMutator
    {
        public RemoveRowMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IEtlRow> MutateRow(IEtlRow row)
        {
            return Enumerable.Empty<IEtlRow>();
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (If == null)
                throw new ProcessParameterNullException(this, nameof(If));
        }
    }
}