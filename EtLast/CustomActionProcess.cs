﻿namespace FizzCode.EtLast
{
    using System;

    public class CustomActionProcess : AbstractExecutableProcess
    {
        public Action<CustomActionProcess> Then { get; set; }

        public CustomActionProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
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