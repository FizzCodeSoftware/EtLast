namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IEnumerable<SlimRow> ExplodeDelegate(ExplodeMutator process, IReadOnlyRow row);

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

        protected override IEnumerable<IEtlRow> MutateRow(IEtlRow row)
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
}