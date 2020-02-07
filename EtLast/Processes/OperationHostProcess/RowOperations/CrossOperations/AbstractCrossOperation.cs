namespace FizzCode.EtLast
{
    public abstract class AbstractCrossOperation : AbstractRowOperation
    {
        public IEvaluable RightProcess { get; set; }

        protected override void PrepareImpl()
        {
            if (RightProcess == null)
                throw new OperationParameterNullException(this, nameof(RightProcess));
        }
    }
}