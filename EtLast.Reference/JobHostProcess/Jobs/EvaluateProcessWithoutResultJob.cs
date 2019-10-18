namespace FizzCode.EtLast
{
    using System.Threading;

    public class EvaluateProcessWithoutResultJob : AbstractJob
    {
        public IFinalProcess ProcessToExecute { get; set; }

        public override void Execute(CancellationTokenSource cancellationTokenSource)
        {
            if (ProcessToExecute == null)
                throw new JobParameterNullException(ProcessToExecute, this, nameof(EvaluateProcessWithoutResultJob.ProcessToExecute));

            ProcessToExecute.Context.Log(LogSeverity.Information, ProcessToExecute, this, null, "evaluating <{InputProcess}>",
                ProcessToExecute.Name);

            ProcessToExecute.EvaluateWithoutResult(ProcessToExecute);
        }
    }
}