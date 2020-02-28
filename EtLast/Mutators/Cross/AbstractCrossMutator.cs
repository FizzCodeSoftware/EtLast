namespace FizzCode.EtLast
{
    public abstract class AbstractCrossMutator : AbstractMutator
    {
        public IEvaluable RightProcess { get; set; }

        protected AbstractCrossMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateMutator()
        {
            if (RightProcess == null)
                throw new ProcessParameterNullException(this, nameof(RightProcess));
        }
    }
}