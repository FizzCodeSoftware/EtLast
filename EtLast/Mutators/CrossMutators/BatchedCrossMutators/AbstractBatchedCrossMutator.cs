namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractBatchedCrossMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public Func<IRow[], IEvaluable> RightProcessCreator { get; set; }

        protected AbstractBatchedCrossMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (RightProcessCreator == null)
                throw new ProcessParameterNullException(this, nameof(RightProcessCreator));
        }
    }
}