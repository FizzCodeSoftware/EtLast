namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class RemoveRowsWithErrorMutator : AbstractMutator
    {
        public RemoveRowsWithErrorMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (!row.HasError())
                yield return row;
        }
    }
}