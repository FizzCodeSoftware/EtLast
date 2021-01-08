namespace FizzCode.EtLast
{
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractCrossMutator : AbstractMutator
    {
        public RowLookupBuilder LookupBuilder { get; init; }

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