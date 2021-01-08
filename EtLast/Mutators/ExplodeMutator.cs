namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public delegate IEnumerable<ISlimRow> ExplodeDelegate(ExplodeMutator process, IReadOnlySlimRow row);

    public class ExplodeMutator : AbstractMutator
    {
        /// <summary>
        /// Default true.
        /// </summary>
        public bool RemoveOriginalRow { get; set; } = true;

        public ExplodeDelegate RowCreator { get; set; }

        public ExplodeMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (!RemoveOriginalRow)
                yield return row;

            var newRows = RowCreator.Invoke(this, row);
            if (newRows != null)
            {
                foreach (var newRow in newRows)
                {
                    yield return Context.CreateRow(this, newRow);
                }
            }
        }

        protected override void ValidateMutator()
        {
            if (RowCreator == null)
                throw new ProcessParameterNullException(this, nameof(RowCreator));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ExplodeMutatorFluent
    {
        public static IFluentProcessMutatorBuilder Explode(this IFluentProcessMutatorBuilder builder, ExplodeMutator mutator)
        {
            return builder.AddMutators(mutator);
        }
    }
}