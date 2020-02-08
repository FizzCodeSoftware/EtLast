namespace FizzCode.EtLast
{
    public abstract class AbstractCrossMutator : AbstractMutator
    {
        public IEvaluable RightProcess { get; set; }

        protected AbstractCrossMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateMutator()
        {
            if (RightProcess == null)
                throw new ProcessParameterNullException(this, nameof(RightProcess));
        }
    }
}