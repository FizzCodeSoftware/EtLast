namespace FizzCode.EtLast
{
    using System;
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractBatchedCrossMutator : AbstractBatchedMutator
    {
        public FilteredRowLookupBuilder LookupBuilder { get; init; }
        public RowKeyGenerator RowKeyGenerator { get; init; }

        protected AbstractBatchedCrossMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (LookupBuilder == null)
                throw new ProcessParameterNullException(this, nameof(LookupBuilder));

            if (RowKeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(RowKeyGenerator));
        }

        protected string GenerateRowKey(IReadOnlyRow row)
        {
            try
            {
                return RowKeyGenerator(row);
            }
            catch (Exception ex)
            {
                throw KeyGeneratorException.Wrap(this, row, ex);
            }
        }
    }
}