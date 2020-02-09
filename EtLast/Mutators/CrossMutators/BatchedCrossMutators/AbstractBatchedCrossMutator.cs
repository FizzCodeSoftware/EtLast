namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractBatchedCrossMutator : AbstractBatchedMutator
    {
        public Func<IRow[], IEvaluable> RightProcessCreator { get; set; }

        protected AbstractBatchedCrossMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
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