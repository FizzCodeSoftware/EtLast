namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractBatchedCrossMutator : AbstractBatchedMutator
    {
        public Func<IRow[], IEvaluable> RightProcessCreator { get; set; }

        protected AbstractBatchedCrossMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (RightProcessCreator == null)
                throw new ProcessParameterNullException(this, nameof(RightProcessCreator));
        }
    }
}