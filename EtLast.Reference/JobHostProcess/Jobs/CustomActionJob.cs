namespace FizzCode.EtLast
{
    using System;
    using System.Threading;

    public class CustomActionJob : AbstractJob
    {
        public Action<IProcess> Then { get; set; }

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            Then?.Invoke(process);
        }
    }
}