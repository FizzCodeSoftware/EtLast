namespace FizzCode.EtLast;

public class ExecutionResult : IExecutionResult
{
    public ExecutionStatusCode Status { get; set; } = ExecutionStatusCode.Success;
    public List<TaskExecutionResult> TaskResults { get; } = [];
    public ContextManifest ContextManifest { get; set; }

    public ExecutionResult()
    {
    }

    public ExecutionResult(ExecutionStatusCode status)
    {
        Status = status;
    }
}