namespace FizzCode.EtLast
{
    public class EvaluateProcessWithoutResultJob : AbstractJob
    {
        public IFinalProcess ProcessToExecute { get; set; }

        public override void Execute()
        {
            if (ProcessToExecute == null)
                throw new JobParameterNullException(ProcessToExecute, this, nameof(ProcessToExecute));

            ProcessToExecute.Context.Log(LogSeverity.Information, ProcessToExecute, this, null, "evaluating <{InputProcess}>",
                ProcessToExecute.Name);

            ProcessToExecute.EvaluateWithoutResult(Process);
        }
    }
}