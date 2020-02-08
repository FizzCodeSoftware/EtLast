namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractKeyBasedCrossMutator : AbstractCrossMutator
    {
        public MatchKeySelector LeftKeySelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }

        protected AbstractKeyBasedCrossMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (LeftKeySelector == null)
                throw new ProcessParameterNullException(this, nameof(LeftKeySelector));

            if (RightKeySelector == null)
                throw new ProcessParameterNullException(this, nameof(RightKeySelector));
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
                var exception = new ProcessExecutionException(this, row, nameof(LeftKeySelector) + " failed");
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
                var exception = new ProcessExecutionException(this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }
    }
}