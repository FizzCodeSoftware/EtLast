namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public class CustomActionProcess : AbstractExecutableProcess
    {
        public Action<CustomActionProcess> Then { get; set; }

        public CustomActionProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            if (Then == null)
                throw new ProcessParameterNullException(this, nameof(Then));
        }

        protected override void ExecuteImpl()
        {
            Then.Invoke(this);
        }
    }
}