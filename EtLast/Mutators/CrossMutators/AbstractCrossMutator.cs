namespace FizzCode.EtLast
{
    public abstract class AbstractCrossMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public IEvaluable RightProcess { get; set; }

        protected AbstractCrossMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (RightProcess == null)
                throw new ProcessParameterNullException(this, nameof(RightProcess));
        }
    }
}