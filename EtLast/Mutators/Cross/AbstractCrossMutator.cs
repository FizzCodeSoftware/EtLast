namespace FizzCode.EtLast
{
    public abstract class AbstractCrossMutator : AbstractMutator
    {
        public RowLookupBuilder LookupBuilder { get; set; }

        protected AbstractCrossMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (LookupBuilder == null)
                throw new ProcessParameterNullException(this, nameof(LookupBuilder));
        }
    }
}