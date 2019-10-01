namespace FizzCode.EtLast
{
    using System.Threading;

    public class EvaluateProcessWithoutResultJob : AbstractJob
    {
        public IFinalProcess Process { get; set; }

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (Process == null)
                throw new JobParameterNullException(process, this, nameof(Process));

            Process.Context.Log(LogSeverity.Information, Process, "({JobName}) evaluating <{InputProcess}>", Name, process.Name);
            Process.EvaluateWithoutResult(process);
        }
    }
}