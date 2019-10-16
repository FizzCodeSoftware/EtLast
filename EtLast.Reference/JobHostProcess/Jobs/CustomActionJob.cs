namespace FizzCode.EtLast
{
    using System;
    using System.Threading;

    public class CustomActionJob : AbstractJob
    {
        public Action<IJob> Then { get; set; }

        public override void Execute(CancellationTokenSource cancellationTokenSource)
        {
            Then?.Invoke(this);
        }
    }
}