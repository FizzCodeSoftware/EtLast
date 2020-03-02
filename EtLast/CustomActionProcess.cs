namespace FizzCode.EtLast
{
    using System;

    public class CustomAction : AbstractExecutable
    {
        public Action<CustomAction> Then { get; set; }

        public CustomAction(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
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