namespace FizzCode.EtLast
{
    using System;

    public sealed class CustomAction : AbstractExecutable
    {
        public Action<CustomAction> Then { get; set; }

        public CustomAction(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (Then == null)
                throw new ProcessParameterNullException(this, nameof(Then));
        }

        protected override void ExecuteImpl()
        {
            try
            {
                Then.Invoke(this);
            }
            catch (Exception ex)
            {
                var exception = new CustomCodeException(this, "error during the execution of custom code", ex);
                throw exception;
            }
        }
    }
}