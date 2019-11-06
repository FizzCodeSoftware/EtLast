namespace FizzCode.EtLast
{
    using System;

    public class CustomActionJob : AbstractJob
    {
        public Action<IJob> Then { get; set; }

        public override void Execute()
        {
            Then?.Invoke(this);
        }
    }
}