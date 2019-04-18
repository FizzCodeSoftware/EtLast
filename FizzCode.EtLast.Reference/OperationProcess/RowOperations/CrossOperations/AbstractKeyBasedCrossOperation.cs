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
            if (LeftKeySelector == null) throw new InvalidOperationParameterException(this, nameof(LeftKeySelector), LeftKeySelector, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (RightKeySelector == null) throw new InvalidOperationParameterException(this, nameof(RightKeySelector), RightKeySelector, InvalidOperationParameterException.ValueCannotBeNullMessage);
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