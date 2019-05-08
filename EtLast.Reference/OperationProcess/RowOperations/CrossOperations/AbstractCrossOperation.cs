namespace FizzCode.EtLast
{
    public abstract class AbstractCrossOperation : AbstractRowOperation
    {
        public IProcess RightProcess { get; set; }

        public override void Prepare()
        {
            if (RightProcess == null)
                throw new OperationParameterNullException(this, nameof(RightProcess));
        }
    }
}