namespace FizzCode.EtLast
{
    public abstract class AbstractCrossOperation : AbstractRowOperation
    {
        public IEvaluable RightProcess { get; set; }

        public override void Prepare()
        {
            base.Prepare();
            if (RightProcess == null)
                throw new OperationParameterNullException(this, nameof(RightProcess));
        }
    }
}