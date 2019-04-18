namespace FizzCode.EtLast
{
    using System.Threading;

    public class EvaluateProcessWithoutResultJob : AbstractJob
    {
        public IFinalProcess Process { get; set; }

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (Process == null) throw new InvalidJobParameterException(process, this, nameof(Process), Process, InvalidOperationParameterException.ValueCannotBeNullMessage);

            Process.EvaluateWithoutResult(process);
        }
    }
}