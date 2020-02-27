namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class RemoveRowWithErrorMutator : AbstractMutator
    {
        public RemoveRowWithErrorMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (!row.HasError())
                yield return row;
        }
    }
}