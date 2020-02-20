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

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            return Enumerable.Empty<IRow>();
        }
    }
}