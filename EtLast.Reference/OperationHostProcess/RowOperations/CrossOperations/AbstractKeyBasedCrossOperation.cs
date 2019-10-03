namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractKeyBasedCrossOperation : AbstractCrossOperation
    {
        public MatchKeySelector LeftKeySelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }

        public override void Prepare()
        {
            base.Prepare();
            if (LeftKeySelector == null)
                throw new OperationParameterNullException(this, nameof(LeftKeySelector));
            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));
        }

        protected string GetLeftKey(IRow row)
        {
            try
            {
                return LeftKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(Process, this, row, nameof(LeftKeySelector) + " failed");
                throw exception;
            }
        }

        protected string GetRightKey(IRow row)
        {
            try
            {
                return RightKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(Process, this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }
    }
}