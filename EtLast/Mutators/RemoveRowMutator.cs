namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class RemoveRowMutator : AbstractMutator
    {
        public RemoveRowMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            return Enumerable.Empty<IRow>();
        }
    }
}