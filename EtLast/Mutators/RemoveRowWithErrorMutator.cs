namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public sealed class RemoveRowWithErrorMutator : AbstractMutator
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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class RemoveRowWithErrorMutatorFluent
    {
        public static IFluentProcessMutatorBuilder RemoveRow(this IFluentProcessMutatorBuilder builder, RemoveRowWithErrorMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}