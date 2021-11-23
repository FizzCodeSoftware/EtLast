namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public delegate IEnumerable<ISlimRow> ExplodeDelegate(IReadOnlyRow row);

    public sealed class ExplodeMutator : AbstractMutator
    {
        /// <summary>
        /// Default true.
        /// </summary>
        public bool RemoveOriginalRow { get; init; } = true;

        public ExplodeDelegate RowCreator { get; init; }

        public ExplodeMutator(IEtlContext context)
            : base(context)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (!RemoveOriginalRow)
                yield return row;

            var newRows = RowCreator.Invoke(row);
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
            return builder.AddMutator(mutator);
        }
    }
}