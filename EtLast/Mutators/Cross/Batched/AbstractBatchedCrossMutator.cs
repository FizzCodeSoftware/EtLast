namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractBatchedCrossMutator : AbstractBatchedMutator
    {
        public FilteredRowLookupBuilder LookupBuilder { get; set; }
        public RowKeyGenerator RowKeyGenerator { get; set; }

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

        protected string GenerateRowKey(IReadOnlySlimRow row)
        {
            try
            {
                return RowKeyGenerator(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new ProcessExecutionException(this, row, nameof(RowKeyGenerator) + " failed");
                throw exception;
            }
        }
    }
}