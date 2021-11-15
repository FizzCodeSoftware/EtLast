namespace FizzCode.EtLast
{
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractCrossMutator : AbstractMutator
    {
        public RowLookupBuilder LookupBuilder { get; init; }

        protected AbstractCrossMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
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