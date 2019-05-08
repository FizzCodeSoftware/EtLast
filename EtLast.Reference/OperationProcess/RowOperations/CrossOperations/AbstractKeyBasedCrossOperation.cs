namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractKeyBasedCrossOperation : AbstractCrossOperation
    {
        public KeySelector LeftKeySelector { get; set; }
        public KeySelector RightKeySelector { get; set; }

        public override void Prepare()
        {
            base.Prepare();
            if (LeftKeySelector == null)
                throw new OperationParameterNullException(this, nameof(LeftKeySelector));
            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));
        }

        protected string GetLeftKey(IProcess process, IRow row)
        {
            try
            {
                return LeftKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(process, this, row, nameof(LeftKeySelector) + " failed");
                throw exception;
            }
        }

        protected string GetRightKey(IProcess process, IRow row)
        {
            try
            {
                return RightKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(process, this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }
    }
}