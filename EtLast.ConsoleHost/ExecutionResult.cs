namespace FizzCode.EtLast.ConsoleHost;

public class ExecutionResult : IExecutionResult
{
    public ExecutionStatusCode Status { get; set; } = ExecutionStatusCode.Success;
    public List<TaskExectionResult> TaskResults { get; } = new List<TaskExectionResult>();

    public ExecutionResult()
    {
    }

    public ExecutionResult(ExecutionStatusCode status)
    {
        Status = status;
    }
}